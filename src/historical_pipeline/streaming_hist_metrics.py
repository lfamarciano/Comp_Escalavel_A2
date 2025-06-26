from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DoubleType,
    LongType,
    IntegerType
)
from delta.tables import DeltaTable
from pathlib import Path
import redis

from publish_to_redis import publish_dataframe_to_redis

import os
from dotenv import load_dotenv
load_dotenv()

# --- Variáveis de ambiente ---
DELTALAKE_BASE_PATH = os.environ.get("DELTALAKE_BASE_PATH", "app/deltalake")
bronze_path = f"{DELTALAKE_BASE_PATH}/bronze"
gold_path = f"{DELTALAKE_BASE_PATH}/gold"
bronze_tv_path = f"{bronze_path}/transacoes_vendas"
bronze_dc_path = f"{bronze_path}/dados_clientes"

# --- Schemas das tabelas ---
GOLD_TABLES_SCHEMAS = {
    "receita_diaria": StructType([
        StructField("data", DateType(), True),
        StructField("segmento_cliente", StringType(), True),
        StructField("receita_total_diaria", DoubleType(), True),
        StructField("numero_de_pedidos", LongType(), True),
        StructField("crescimento_receita_d-1", DoubleType(), True)
    ]),
    
    "produtos_mais_vendidos": StructType([
        # Schema para a métrica de produtos mais vendidos
        StructField("ano", IntegerType(), True),
        StructField("trimestre", IntegerType(), True),
        StructField("rank", IntegerType(), True),
        StructField("id_produto", StringType(), True), # Assumindo que o ID do produto é string/UUID
        StructField("nome_produto", StringType(), True),
        StructField("unidades_vendidas", LongType(), True)
    ])
}

def upsert_daily_metrics(micro_batch_df: DataFrame, epoch_id: int):
    """
    Função chamada para cada micro-lote do streaming.
    Calcula o crescimento e faz o MERGE na tabela Delta Gold e publica no Redis.
    """
    # Pegando sessão espark que criou esse micro_batch
    spark = micro_batch_df.sparkSession
    
    print(f"\n--- Processando Micro-Lote ID: {epoch_id} ---")
    
    # Micro-batch df que contém os valores do dia mais recente recém
    # calculados
    micro_batch_df.persist()
    
    # gold_path = "deltalake/gold/receita_diaria" # local
    gold_table_path = f"{gold_path}/receita_diaria" # local

    if micro_batch_df.count() == 0:
        print(f"[Micro-Lote ID: {epoch_id}] Lote Vazio. Pulando.")
        return

    gold_table = DeltaTable.forPath(spark, gold_table_path)

    # Otimização filtrando dados de acordo com o lote atual
    batch_dates_segments = micro_batch_df.select("data", "segmento_cliente").distinct().collect()
    
    if not batch_dates_segments:
        print(f"[Micro-Lote ID: {epoch_id}] Lote sem dados após distinct. Pulando.")
        micro_batch_df.unpersist()
        return

    # Filtro para busca apenas de dados do dia anterior
    yesterday_filter = "OR ".join(
        [f"(segmento_cliente = '{row.segmento_cliente}' AND data = '{row.data.isoformat()}' - INTERVAL 1 DAY)" for row in batch_dates_segments]
    )

    # Lendo apenas os dados de ontem da tabela Gold que são relevantes para o lote atual
    yesterday_data_df = gold_table.toDF().filter(yesterday_filter) \
        .select(
            F.col("data").alias("data_dia_anterior"), 
            F.col("segmento_cliente"), 
            F.col("receita_total_diaria").alias("receita_dia_anterior")
        )
    
    # Evitando ambiguidade na operação de join
    df_batch = micro_batch_df.alias("batch")
    df_yesterday = yesterday_data_df.alias("yesterday")
    join_condition = (
        (df_batch.segmento_cliente == df_yesterday.segmento_cliente) &
        (df_batch.data == df_yesterday.data_dia_anterior + F.expr("INTERVAL 1 DAY"))
    )

    # Juntando dados do lote com os dados do D-1
    new_data_with_growth = micro_batch_df.join(
        df_yesterday,
        join_condition,
        "left"
    ).select(
        df_batch["*"],
        df_yesterday["receita_dia_anterior"]
    )
    
    # Calculando coluna do crescimento
    final_df_to_write = new_data_with_growth.withColumn(
        "crescimento_receita_d-1",
        F.when(
            (F.col("receita_dia_anterior").isNotNull()) & (F.col("receita_dia_anterior") > 0),
            (F.col("receita_total_diaria") - F.col("receita_dia_anterior")) / F.col("receita_dia_anterior")
        ).otherwise(None)
    ).select(
        "data",
        "segmento_cliente",
        "receita_total_diaria",
        "numero_de_pedidos",
        "crescimento_receita_d-1"
    )

    # Fazendo o MERGE na tabela Delta Gold
    print("[Delta Merge] Realizando MERGE na tabela Gold...")
    gold_table.alias("target").merge(
        source=final_df_to_write.alias("source"),
        condition="target.data = source.data AND target.segmento_cliente = source.segmento_cliente"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print("[Delta Merge] MERGE concluído.")

    # Publicando os dados mais recentes no Redis
    latest_metrics_df = spark.read.format("delta").load(gold_table_path) \
        .filter(F.col("data") >= F.current_date() - F.expr("INTERVAL 30 DAY")) \
        .orderBy(F.col("data").desc(), "segmento_cliente")
    
    # Publicando o dataframe inteiro como um JSON no Redis
    publish_dataframe_to_redis(latest_metrics_df, "historical:daily_revenue_metrics")
    print("[Redis] Métricas diárias publicadas no Redis.")

    micro_batch_df.unpersist()

def boostrap_gold_layer(spark: SparkSession, gold_path: str, schemas_config: dict):
    """
    Verifica a existência da tabela gold e a cria caso não exista.
    """

    for table_name, schema in schemas_config.items():
        table_path = f"{gold_path}/{table_name}"

        if DeltaTable.isDeltaTable(spark, table_path):
            print(f"[Bootstrap] Tabela Gold '{table_path}' já existe. Nada a fazer.")
            return

        else:
            print(f"[Bootstrap] Tabela Gold '{table_path}' não encontrada. Criando tabela vazia.")

            # Criando tabela a partir de schema dado
            empty_df = spark.createDataFrame([], schema)
            # empty_df.write.format("delta").save(table_path).over
            empty_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)

            print(f"[Bootstrap] Tabela Gold {table_path} criada com sucesso.")

def main ():
    # --- Inicializando: sessão spark e bootstrap ---
    spark = SparkSession.builder \
        .appName("StreamingMetricsWorker") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    boostrap_gold_layer(spark, gold_path, GOLD_TABLES_SCHEMAS)
    
    # --- Lendo dados da camada bronze como stream ---
    streaming_options = {"ignoreDeletes": "true", "ignoreChanges": "true"}
    transacoes_stream_df = spark.readStream.format("delta").options(**streaming_options).load(bronze_tv_path)
    # Tabela de clientes não muda, então lemos como estática
    clientes_static_df = spark.read.format("delta").load(bronze_dc_path)

    # Juntando stream com tabela estática de clientes e calculando agregado diário
    daily_revenue_stream_df = transacoes_stream_df \
        .join(clientes_static_df, "id_usuario", "left") \
        .withColumn("data", F.to_date(F.col("data_compra"))) \
        .groupBy("data", "segmento_cliente") \
        .agg(
            F.sum("valor_total_compra").alias("receita_total_diaria"),
            F.count("id_transacao").alias("numero_de_pedidos")
        )

    # Definindo o caminho do checkpoint de forma absoluta
    checkpoint_path = f"{DELTALAKE_BASE_PATH}/checkpoints/streaming_hist_metrics"

    # Calculando crescimento com upsert
    daily_revenue_query = daily_revenue_stream_df.writeStream \
        .foreachBatch(upsert_daily_metrics) \
        .outputMode("update") \
        .option("checkpointLocation", checkpoint_path) \
        .start()

    print("--- Todos os streams foram iniciados ---")
    daily_revenue_query.awaitTermination()

if __name__ == "__main__":
    spark = SparkSession.getActiveSession()
    main()