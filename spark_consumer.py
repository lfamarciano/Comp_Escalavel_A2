import os
import json
import redis
import pandas as pd # Importa a biblioteca pandas
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, count, approx_count_distinct, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# --- 1. CONFIGURAÇÃO PARA WINDOWS ---
os.environ['HADOOP_HOME'] = 'C:\\hadoop'

# --- 2. Configuração da Sessão Spark ---
spark = (
    SparkSession.builder.appName("EcommerceConsumerIncrementalDebug")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.sql.streaming.ui.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("Sessão Spark iniciada.")
print(f"Acesse a UI do Spark em: http://{spark.conf.get('spark.driver.host')}:4040")

# --- 3. Definição dos Schemas ---
schema_transacoes = StructType([
    StructField("id_transacao", StringType(), True), StructField("id_pedido", StringType(), True),
    StructField("id_usuario", IntegerType(), True), StructField("nome_usuario", StringType(), True),
    StructField("id_produto", IntegerType(), True), StructField("categoria", StringType(), True),
    StructField("item", StringType(), True), StructField("valor_total_compra", DoubleType(), True),
    StructField("quantidade_produto", IntegerType(), True), StructField("data_compra", TimestampType(), True),
    StructField("metodo_pagamento", StringType(), True), StructField("status_pedido", StringType(), True),
    StructField("id_carrinho", StringType(), True)
])
schema_eventos = StructType([
    StructField("id_evento", StringType(), True), StructField("id_usuario", IntegerType(), True),
    StructField("id_sessao", StringType(), True), StructField("tipo_evento", StringType(), True),
    StructField("id_carrinho", StringType(), True), StructField("id_produto", IntegerType(), True),
    StructField("timestamp_evento", TimestampType(), True),
])

# --- 4. Leitura e Parsing dos Streams ---
KAFKA_BROKER_URL = "localhost:9092"
CHECKPOINT_BASE_PATH = "C:/tmp/spark_checkpoints"

df_transacoes_raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BROKER_URL).option("subscribe", "transacoes_vendas").option("startingOffsets", "latest").load()
df_transacoes = df_transacoes_raw.select(from_json(col("value").cast("string"), schema_transacoes).alias("data")).select("data.*")

df_eventos_raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BROKER_URL).option("subscribe", "eventos_web").option("startingOffsets", "latest").load()
df_eventos = df_eventos_raw.select(from_json(col("value").cast("string"), schema_eventos).alias("data")).select("data.*")

print("Streams do Kafka sendo lidos e parseados.")

# --- 5. Lógica de Escrita no Redis (LÓGICA CORRIGIDA E MAIS ROBUSTA) ---
def write_to_redis(df, metric_name):
    """Escreve um DataFrame de um micro-lote no Redis usando Pandas."""
    if not df.isEmpty():
        try:
            r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
            
            # Converte o DataFrame do Spark para um DataFrame do Pandas
            pandas_df = df.toPandas()
            # Converte o DataFrame do Pandas para uma lista de dicionários
            rows = pandas_df.to_dict('records')

            payload = json.dumps(rows[0] if len(rows) == 1 else rows)
            redis_key = f"realtime:{metric_name}"
            r.set(redis_key, payload)
            print(f"Métrica '{redis_key}' atualizada no Redis.")
        except Exception as e:
            print(f"ERRO ao escrever no Redis para a métrica '{metric_name}': {e}")


# --- 6. Cálculo das Métricas e Início das Queries ---
# Começando apenas com a primeira query para confirmar a estabilidade
# === Query 1: Métricas Globais (ATIVADA) ===
metricas_globais = df_transacoes.agg(
    _sum("valor_total_compra").alias("receita_total_global"),
    approx_count_distinct("id_pedido").alias("pedidos_totais_global")
).selectExpr(
    "receita_total_global",
    "pedidos_totais_global",
    "receita_total_global / pedidos_totais_global as ticket_medio_global"
)
streaming_query_globais = metricas_globais.writeStream.outputMode("complete").foreachBatch(lambda df, epoch_id: write_to_redis(df, "metricas_globais")).option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/globais").start()
print("Query para 'Métricas Globais' iniciada.")

# --- Manter a aplicação rodando ---
print("\nUma query de streaming foi iniciada. Pressione Ctrl+C para parar.")
spark.streams.awaitAnyTermination()

