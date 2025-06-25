# Arquivo: src/spark_consumer.py (versão final corrigida)

import os
import json
import redis
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, count, approx_count_distinct, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Configurações
try:
    from config.local import KAFKA_HOST, REDIS_HOST, TRANSACTIONS_TOPIC, WEB_EVENTS_TOPIC
except ImportError:
    KAFKA_HOST = os.environ.get('KAFKA_HOST', 'kafka:9092')
    REDIS_HOST = os.environ.get('REDIS_HOST', 'redis')
    TRANSACTIONS_TOPIC = os.environ.get('TRANSACTIONS_TOPIC', 'transacoes_vendas')
    WEB_EVENTS_TOPIC = os.environ.get('WEB_EVENTS_TOPIC', 'eventos_web')

# --- 1. Configuração da Sessão Spark ---
spark = (
    SparkSession.builder.appName("EcommerceConsumerFinal")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("Sessão Spark iniciada.")

# --- 3. Definição dos Schemas (CORRIGIDOS E COMPLETOS) ---
schema_transacoes = StructType([
    StructField("id_pedido", StringType(), True),
    StructField("id_usuario", IntegerType(), True),
    StructField("nome_usuario", StringType(), True),
    StructField("id_produto", IntegerType(), True),
    StructField("categoria", StringType(), True),
    StructField("item", StringType(), True),
    StructField("valor_total_compra", DoubleType(), True),
    StructField("quantidade_produto", IntegerType(), True),
    StructField("data_compra", TimestampType(), True),
    StructField("metodo_pagamento", StringType(), True),
    StructField("status_pedido", StringType(), True),
    StructField("id_carrinho", StringType(), True) # <-- O campo que faltava!
])

schema_eventos = StructType([
    StructField("id_usuario", IntegerType(), True),
    StructField("id_sessao", StringType(), True),
    StructField("tipo_evento", StringType(), True),
    StructField("id_carrinho", StringType(), True),
    StructField("id_produto", IntegerType(), True),
    StructField("timestamp_evento", TimestampType(), True)
])

# --- 4. Leitura dos Streams ---
CHECKPOINT_BASE_PATH = "/tmp/spark_checkpoints"
df_transacoes_raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_HOST).option("subscribe", TRANSACTIONS_TOPIC).option("startingOffsets", "latest").load()
df_transacoes = df_transacoes_raw.select(from_json(col("value").cast("string"), schema_transacoes).alias("data")).select("data.*")
df_eventos_raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_HOST).option("subscribe", WEB_EVENTS_TOPIC).option("startingOffsets", "latest").load()
df_eventos = df_eventos_raw.select(from_json(col("value").cast("string"), schema_eventos).alias("data")).select("data.*")
print("Streams do Kafka sendo lidos e parseados.")


# --- 5. Lógica de Escrita com foreachPartition (sem alterações) ---
def write_partition_to_temp_redis_list(partition_iterator, temp_key):
    r = redis.StrictRedis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
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
        r = redis.StrictRedis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
        json_strings_list = r.lrange(temp_key, 0, -1)
        if json_strings_list:
            is_single_row_metric = "global" in metric_name or "total" in metric_name
            payload = json_strings_list[0] if is_single_row_metric else f"[{','.join(json_strings_list)}]"
            r.set(final_key, payload)
            print(f"Métrica '{final_key}' atualizada no Redis (método foreachPartition).")
        r.delete(temp_key)
    except Exception as e:
        print(f"ERRO no driver ao processar lote do Redis para '{metric_name}': {e}")

# --- 6. Cálculo das Métricas e Início das Queries (sem alterações) ---
TRIGGER_INTERVAL = "10 seconds"

# Query 1: Métricas Globais
metricas_globais = df_transacoes.agg(_sum("valor_total_compra").alias("receita_total_global"), approx_count_distinct("id_pedido").alias("pedidos_totais_global")).selectExpr("receita_total_global", "pedidos_totais_global", "receita_total_global / pedidos_totais_global as ticket_medio_global")
query_globais = metricas_globais.writeStream.outputMode("complete").foreachBatch(lambda df, id: write_to_redis_with_foreach_partition(df, "metricas_globais")).option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/globais").trigger(processingTime=TRIGGER_INTERVAL).start()

# Query 2: Receita por Categoria
receita_por_categoria = df_transacoes.groupBy("categoria").agg(_sum("valor_total_compra").alias("receita"))
query_categoria = receita_por_categoria.writeStream.outputMode("complete").foreachBatch(lambda df, id: write_to_redis_with_foreach_partition(df, "receita_por_categoria")).option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/categoria").trigger(processingTime=TRIGGER_INTERVAL).start()

# Query 3: Produtos Mais Vendidos
contagem_produtos = df_transacoes.groupBy("item").agg(count("*").alias("total_vendido"))
def processa_e_escreve_top_n(df, epoch_id, n=5):
    if not df.isEmpty():
        top_n_df = df.orderBy(desc("total_vendido")).limit(n)
        write_to_redis_with_foreach_partition(top_n_df, "top_5_produtos")
query_top_produtos = contagem_produtos.writeStream.outputMode("complete").foreachBatch(lambda df, id: processa_e_escreve_top_n(df, id, n=5)).option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/top_produtos").trigger(processingTime=TRIGGER_INTERVAL).start()

# Contadores para Taxas
total_carrinhos_criados = df_eventos.filter(col("tipo_evento") == "carrinho_criado").agg(count("*").alias("total"))
query_criados = total_carrinhos_criados.writeStream.outputMode("complete").foreachBatch(lambda df, id: write_to_redis_with_foreach_partition(df, "total_carrinhos_criados")).option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/carrinhos_criados").trigger(processingTime=TRIGGER_INTERVAL).start()

# A query que estava quebrando, agora vai funcionar!
total_carrinhos_convertidos = df_transacoes.agg(approx_count_distinct("id_carrinho").alias("total"))
query_convertidos = total_carrinhos_convertidos.writeStream.outputMode("complete").foreachBatch(lambda df, id: write_to_redis_with_foreach_partition(df, "total_carrinhos_convertidos")).option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/carrinhos_convertidos").trigger(processingTime=TRIGGER_INTERVAL).start()

print("\nTodas as queries de streaming foram iniciadas com foreachPartition. Pressione Ctrl+C para parar.")
spark.streams.awaitAnyTermination()