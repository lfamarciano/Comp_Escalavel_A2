from pyspark.sql import SparkSession, Window
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
import redis

from pathlib import Path
import json
import uuid

from publish_to_redis import REDIS_HOST, REDIS_PORT

def write_partition_to_temp_redis_list(partition_iterator, temp_key):
    r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True, ssl=True, ssl_cert_reqs=None)
    json_strings = [json.dumps(row.asDict()) for row in partition_iterator]
    if json_strings:
        r.rpush(temp_key, *json_strings)

def write_to_redis_with_foreach_partition(df, metric_name):
    if df.isEmpty():
        return
    batch_id = str(uuid.uuid4())
    temp_key = f"temp:{metric_name}:{batch_id}"
    final_key = f"realtime:{metric_name}"
    
    df.foreachPartition(lambda p: write_partition_to_temp_redis_list(p, temp_key))

    try:
        r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        json_strings_list = r.lrange(temp_key, 0, -1)
        if json_strings_list:
            is_single_row_metric = "global" in metric_name or "total" in metric_name
            payload = json_strings_list[0] if is_single_row_metric else f"[{','.join(json_strings_list)}]"
            r.set(final_key, payload)
            print(f"Métrica '{final_key}' atualizada no Redis (método foreachPartition).")
        r.delete(temp_key)
    except Exception as e:
        print(f"ERRO no driver ao processar lote do Redis para '{metric_name}': {e}")

import os
from dotenv import load_dotenv
load_dotenv()

# --- Variáveis de ambiente ---
DELTALAKE_BASE_PATH = os.environ.get("DELTALAKE_BASE_PATH", "app/deltalake")
bronze_path = f"{DELTALAKE_BASE_PATH}/bronze"
gold_path = f"{DELTALAKE_BASE_PATH}/gold"
bronze_tv_path = f"{bronze_path}/transacoes_vendas"
bronze_dc_path = f"{bronze_path}/dados_clientes"
bronze_cp_path = f"{bronze_path}/catalogo_produtos"
bronze_ew_path = f"{bronze_path}/eventos_web"

# --- Schemas das tabelas ---
GOLD_TABLES_SCHEMAS = {
    "receita_diaria": StructType([
        StructField("data", DateType(), True),
        StructField("segmento_cliente", StringType(), True),
        StructField("receita_total_diaria", DoubleType(), True),
        StructField("numero_de_pedidos", LongType(), True),
        StructField("crescimento_receita_d-1", DoubleType(), True)
    ]),
    
    "produtos_top10_trimestral": StructType([
        StructField("ano", IntegerType(), False),
        StructField("trimestre", IntegerType(), False),
        StructField("rank", IntegerType(), False),
        StructField("id_produto", StringType(), False),
        StructField("nome_produto", StringType(), True), # Vem do join
        StructField("unidades_vendidas", LongType(), True)
    ]),

    "produtos_agregados_trimestral": StructType([
        StructField("ano", IntegerType(), False),
        StructField("trimestre", IntegerType(), False),
        StructField("id_produto", StringType(), False),
        StructField("unidades_vendidas", LongType(), True)
    ]),

    "carrinhos_criados": StructType([
        StructField("id_carrinho", StringType(), False)
    ]),

    "carrinhos_finalizados": StructType([
        StructField("id_carrinho", StringType(), False)
    ])
}

def upsert_daily_revenue(micro_batch_df: DataFrame, epoch_id: int):
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
    # publish_dataframe_to_redis(latest_metrics_df, "historical:daily_revenue_metrics")
    write_to_redis_with_foreach_partition(latest_metrics_df, "historical:daily_revenue_metrics")
    print("[Redis] Métricas diárias publicadas no Redis.")

    micro_batch_df.unpersist()

def upsert_most_sold_products(micro_batch_df: DataFrame, epoch_id: int):
    """
    Função foreachBatch para calcular e atualizar o ranking de produtos mais vendidos por trimestre.
    """
    spark = micro_batch_df.sparkSession
    gold_agregados_path = f"{gold_path}/produtos_agregados_trimestral"
    gold_top10_path = f"{gold_path}/produtos_top10_trimestral"

    print(f"\n--- [Produtos] Processando Micro-Lote ID: {epoch_id} ---")

    # Pré-processando o micro-lote para obter as vendas por produto/trimestre
    updates_df = micro_batch_df.withColumn("ano", F.year("data_compra")) \
                                .withColumn("trimestre", F.quarter("data_compra")) \
                                .groupBy("ano", "trimestre", "id_produto") \
                                .agg(F.sum("quantidade_produto").alias("unidades_vendidas_update"))
    
    updates_df.persist()

    if updates_df.count() == 0:
        print(f"[Produtos] Lote vazio. Pulando.")
        updates_df.unpersist()
        return

    # Fazendo merge na tabela de agregados para atualizar os totais
    tabela_agregados = DeltaTable.forPath(spark, gold_agregados_path)
    
    print("[Produtos] Realizando MERGE na tabela de agregados...")
    tabela_agregados.alias("target").merge(
        source=updates_df.alias("source"),
        condition="target.ano = source.ano AND target.trimestre = source.trimestre AND target.id_produto = source.id_produto"
    ).whenMatchedUpdate(
        set={"unidades_vendidas": F.col("target.unidades_vendidas") + F.col("source.unidades_vendidas_update")}
    ).whenNotMatchedInsert(
        values={
            "ano": "source.ano",
            "trimestre": "source.trimestre",
            "id_produto": "source.id_produto",
            "unidades_vendidas": "source.unidades_vendidas_update"
        }
    ).execute()
    print("[Produtos] MERGE de agregados concluído.")
    
    # -- Recalculando o TOP 10 para os trimestres que foram atualizados --
    # Identifica os trimestres afetados neste lote
    trimestres_afetados = updates_df.select("ano", "trimestre").distinct().collect()
    filtro_trimestres = " OR ".join([f"(ano = {row.ano} AND trimestre = {row.trimestre})" for row in trimestres_afetados])
    # Lendo a tabela de agregados completa, mas filtrada para os trimestres relevantes
    agregados_atualizados_df = spark.read.format("delta").load(gold_agregados_path).filter(filtro_trimestres)
    # Lendo a tabela estática de produtos para obter os nomes
    produtos_static_df = spark.read.format("delta").load(bronze_cp_path)
    # Janela para calcular o ranking
    window_spec = Window.partitionBy("ano", "trimestre").orderBy(F.desc("unidades_vendidas"))
    # Calcula o ranking, junta para obter nomes e filtra o TOP 10
    top_10_df = agregados_atualizados_df.withColumn("rank", F.row_number().over(window_spec)) \
                                        .filter(F.col("rank") <= 10) \
                                        .join(produtos_static_df, "id_produto", "left") \
                                        .select(
                                            "ano", "trimestre", "rank", "id_produto",
                                            "nome_produto", "unidades_vendidas"
                                        )
    
    # Salvando na tabela gold, usando merge novamente para atualizar rankings dos semestres
    print("[Produtos] Salvando ranking TOP 10 na tabela final...")
    DeltaTable.forPath(spark, gold_top10_path).alias("target").merge(
        source=top_10_df.alias("source"),
        condition="target.ano = source.ano AND target.trimestre = source.trimestre AND target.id_produto = source.id_produto"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    # Publicando no Redis
    latest_top10_df = spark.read.format("delta").load(gold_top10_path).orderBy(F.desc("ano"), F.desc("trimestre"), "rank")
    # publish_dataframe_to_redis(latest_top10_df, "historical:top10_products_quarterly")
    write_to_redis_with_foreach_partition(latest_top10_df, "historical:top10_products_quarterly")
    print("[Produtos] Ranking TOP 10 publicado no Redis.")
    
    updates_df.unpersist()

def update_created_carts(micro_batch_df: DataFrame, epoch_id: int):
    """
    Registra os IDs de carrinhos recém-criados na tabela de estado 'carrinhos_criados'.
    """
    spark = micro_batch_df.sparkSession
    gold_created_carts_path = f"{gold_path}/carrinhos_criados"
    
    print(f"\n--- [Carrinhos Criados] Processando Micro-Lote ID: {epoch_id} ---")

    # Apenas insere os novos IDs se eles ainda não existirem.
    DeltaTable.forPath(spark, gold_created_carts_path).alias("target").merge(
        source=micro_batch_df.alias("source"),
        condition="target.id_carrinho = source.id_carrinho"
    ).whenNotMatchedInsertAll().execute()
    
    print(f"[Carrinhos Criados] {micro_batch_df.count()} novos registros de carrinhos processados.")


def update_finished_carts_and_calc_rate(micro_batch_df: DataFrame, epoch_id: int):
    """
    Registra os IDs de carrinhos finalizados e, em seguida, recalcula e publica
    a taxa de abandono de carrinho global.
    """
    spark = micro_batch_df.sparkSession
    gold_created_carts_path = f"{gold_path}/carrinhos_criados"
    gold_finished_carts_path = f"{gold_path}/carrinhos_finalizados"

    print(f"\n--- [Carrinhos Finalizados] Processando Micro-Lote ID: {epoch_id} ---")

    # Atualizando tabela de estado de carrinhos finalizados
    DeltaTable.forPath(spark, gold_finished_carts_path).alias("target").merge(
        source=micro_batch_df.alias("source"),
        condition="target.id_carrinho = source.id_carrinho"
    ).whenNotMatchedInsertAll().execute()
    
    print(f"[Carrinhos Finalizados] {micro_batch_df.count()} novos carrinhos finalizados registrados.")

    # Recalculando taxa de abandono global
    print("[Taxa Abandono] Calculando a taxa global...")
    # Lendo estado completo de ambas as tabelas
    all_created_carts_df = spark.read.format("delta").load(gold_created_carts_path)
    all_finished_carts_df = spark.read.format("delta").load(gold_finished_carts_path)
    all_created_carts_df.cache()
    total_carrinhos_criados = all_created_carts_df.count()
    if total_carrinhos_criados == 0:
        taxa_abandono = 0.0
    else:
        # Encontrando carrinhos criados mas nunca finalizados
        abandoned_carts_df = all_created_carts_df.join(
            all_finished_carts_df,
            on="id_carrinho",
            how="left_anti"
        )
        total_carrinhos_abandonados = abandoned_carts_df.count()
        
        taxa_abandono = (total_carrinhos_abandonados / total_carrinhos_criados) * 100

    all_created_carts_df.unpersist()
    
    # Publicando a métrica no Redis
    print(f"[Taxa Abandono] Taxa de Abandono de Carrinho Atual: {taxa_abandono:.2f}%")
    # Criando dataframe para utilziar foreachPartition
    rate_df = spark.createDataFrame(
        [{"value": f"{taxa_abandono:.2f}"}],
        StructType([StructField("value", StringType(), True)])
    )
    
    # publish_metric_to_redis(f"{taxa_abandono:.2f}", "historical:abandoned_cart_rate")
    write_to_redis_with_foreach_partition(rate_df, "historical:abandoned_cart_rate")
    print("[Taxa Abandono] Métrica publicada no Redis.")

def boostrap_gold_layer(spark: SparkSession, gold_path: str, schemas_config: dict):
    """
    Verifica a existência da tabela gold e a cria caso não exista.
    """

    for table_name, schema in schemas_config.items():
        table_path = f"{gold_path}/{table_name}"

        if DeltaTable.isDeltaTable(spark, table_path):
            print(f"[Bootstrap] Tabela Gold '{table_path}' já existe. Nada a fazer.")
            continue

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
    # -- Dataframes de stream --
    transacoes_stream_df = spark.readStream.format("delta").options(**streaming_options).load(bronze_tv_path)
    eventos_web_stream_df = spark.readStream.format("delta").options(**streaming_options).load(bronze_ew_path)
    # -- Dataframes estáticos --
    # Tabela de clientes não muda, então lemos como estática
    clientes_static_df = spark.read.format("delta").load(bronze_dc_path)
    # Assim como a tabela de produtos
    produtos_static_df = spark.read.format("delta").load(bronze_cp_path)

    # --- Streams ---
    # Definindo o caminho do checkpoint de forma absoluta
    # -- Receita diária --
    # Juntando stream com tabela estática de clientes e calculando agregado diário
    checkpoint_receita_path = f"{DELTALAKE_BASE_PATH}/checkpoints/streaming_receita_metrics"
    daily_revenue_stream_df = transacoes_stream_df \
        .join(clientes_static_df, "id_usuario", "left") \
        .withColumn("data", F.to_date(F.col("data_compra"))) \
        .groupBy("data", "segmento_cliente") \
        .agg(
            F.sum("valor_total_compra").alias("receita_total_diaria"),
            F.count("id_transacao").alias("numero_de_pedidos")
        )
    # Calculando crescimento com upsert
    daily_revenue_query = daily_revenue_stream_df.writeStream \
        .foreachBatch(upsert_daily_revenue) \
        .outputMode("update") \
        .option("checkpointLocation", checkpoint_receita_path) \
        .start()
    # -- Produtos mais vendidos --
    checkpoint_produtos_path = f"{DELTALAKE_BASE_PATH}/checkpoints/streaming_produtos_metrics"
    most_sold_products_query = transacoes_stream_df.writeStream \
        .foreachBatch(upsert_most_sold_products) \
        .outputMode("update") \
        .option("checkpointLocation", checkpoint_produtos_path) \
        .start()
    # -- Taxa de abandono de carrinho --
    # - Carrinhos criados -
    checkpoint_created_carts_path = f"{DELTALAKE_BASE_PATH}/checkpoints/streaming_created_carts"
    created_carts_stream_df = eventos_web_stream_df \
        .filter(F.col("tipo_evento") == "carrinho_criado") \
        .select("id_carrinho") \
        .distinct()
    created_carts_query = created_carts_stream_df.writeStream \
        .foreachBatch(update_created_carts) \
        .outputMode("update") \
        .option("checkpointLocation", checkpoint_created_carts_path) \
        .start()
    # - Carrinhos finalizados e cálculo da taxa -
    finished_carts_stream_df = transacoes_stream_df \
        .select("id_carrinho") \
        .distinct()
    checkpoint_finished_carts_path = f"{DELTALAKE_BASE_PATH}/checkpoints/streaming_finished_carts"
    finished_carts_query = finished_carts_stream_df.writeStream \
        .foreachBatch(update_finished_carts_and_calc_rate) \
        .outputMode("update") \
        .option("checkpointLocation", checkpoint_finished_carts_path) \
        .start()

    print("\n--- Todos os streams foram iniciados ---")
    daily_revenue_query.awaitTermination()
    most_sold_products_query.awaitTermination()
    created_carts_query.awaitTermination()
    finished_carts_query.awaitTermination()

if __name__ == "__main__":
    main()
    # for table_name, schema in GOLD_TABLES_SCHEMAS.items():
    #     print("")
    #     print(table_name, schema)