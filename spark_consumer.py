# ANTES DE EXECUTAR O CONSUMIDOR, CERTIFIQUE-SE DE QUE O KAFKA ESTÁ RODANDO E OS TÓPICOS 'transacoes_vendas' E 'eventos_web' ESTÃO CRIADOS.
# PARA ISSO BASTA RODAR O SCRIPT 'producer.py' ANTES DESTE CONSUMIDOR.
# PARA INICIAR O CONSUMIDOR, EXECUTE O SEGUINTE COMANDO NO TERMINAL:
# > spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 spark_consumer.py  
# PARA VERIFICAR SE AS MÉTRICAS ESTÃO SENDO ATUALIZADAS NO REDIS, USE O COMANDO:
# > docker exec -it redis redis-cli  
# > KEYS *
# > GET <nome_da_métrica> / PARA VER O VALOR DE UMA MÉTRICA ESPECÍFICA

import os
import json
import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, count, approx_count_distinct, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


#  1. CONFIGURAÇÃO PARA WINDOWS
os.environ['HADOOP_HOME'] = 'C:\\hadoop'

#  2. Configuração do Spark 
spark = (
    SparkSession.builder.appName("EcommerceConsumerIncrementalDebug")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
    .config("spark.python.worker.connect.timeout", "120000ms") 
    .config("spark.sql.streaming.ui.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("Sessão Spark iniciada.")
print(f"Acesse a UI do Spark em: http://{spark.conf.get('spark.driver.host')}:4040")

#  3. Definição dos Schemas 
schema_transacoes = StructType([
    StructField("id_pedido", StringType(), True),
    StructField("id_usuario", IntegerType(), True), StructField("nome_usuario", StringType(), True),
    StructField("id_produto", IntegerType(), True), StructField("categoria", StringType(), True),
    StructField("item", StringType(), True), StructField("valor_total_compra", DoubleType(), True),
    StructField("quantidade_produto", IntegerType(), True), StructField("data_compra", TimestampType(), True),
    StructField("metodo_pagamento", StringType(), True), StructField("status_pedido", StringType(), True),
    StructField("id_carrinho", StringType(), True)
])

schema_eventos = StructType([
    StructField("id_usuario", IntegerType(), True),
    StructField("id_sessao", StringType(), True), StructField("tipo_evento", StringType(), True),
    StructField("id_carrinho", StringType(), True), StructField("id_produto", IntegerType(), True),
    StructField("timestamp_evento", TimestampType(), True),
])

#  4. Leitura e Parsing dos Streams 
KAFKA_BROKER_URL = "localhost:9092"
CHECKPOINT_BASE_PATH = "C:/tmp/spark_checkpoints"

# Se inscreve nos tópicos do Kafka e lê os dados
df_transacoes_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER_URL) \
    .option("subscribe", "transacoes_vendas").option("startingOffsets", "latest") \
    .load()
    
df_transacoes = df_transacoes_raw.select(from_json(col("value").cast("string"), schema_transacoes) \
    .alias("data")) \
    .select("data.*")

df_eventos_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER_URL) \
    .option("subscribe", "eventos_web") \
    .option("startingOffsets", "latest") \
    .load()
    
df_eventos = df_eventos_raw.select(from_json(col("value") \
    .cast("string"), schema_eventos).alias("data")) \
    .select("data.*")

print("Streams do Kafka sendo lidos e parseados.")

# 5. Lógica de Escrita no Redis mais eficiente
def write_partition_to_redis(partition_iterator, metric_name):
    """
    Função executada em cada trabalhador do Spark.
    Abre uma única ligação ao Redis por partição e escreve os dados.
    """
    r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
    # Usa um pipeline para enviar múltiplos comandos de uma vez, otimizando a rede
    pipe = r.pipeline()
    
    # Converte cada linha da partição num dicionário e adiciona ao pipeline
    rows = [row.asDict() for row in partition_iterator]
    
    if rows:
        payload = json.dumps(rows[0] if len(rows) == 1 else rows)
        redis_key = f"realtime:{metric_name}"
        pipe.set(redis_key, payload)
        pipe.execute()

def process_batch(df, metric_name):
    """Função chamada pelo foreachBatch para iniciar a escrita em paralelo."""
    df.foreachPartition(lambda p: write_partition_to_redis(p, metric_name))
    print(f"Lote para a métrica 'realtime:{metric_name}' processado.")


#  6. Cálculo das Métricas e Início das Queries 
# TRIGGER_INTERVAL = "15 seconds"

# === Query 1: Métricas Globais (Receita, Pedidos, Ticket Médio) ===
metricas_globais = df_transacoes.agg(
    _sum("valor_total_compra").alias("receita_total_global"),
    approx_count_distinct("id_pedido").alias("pedidos_totais_global")
).selectExpr("receita_total_global", "pedidos_totais_global", "receita_total_global / pedidos_totais_global as ticket_medio_global")

query_globais = metricas_globais.writeStream.outputMode("complete") \
    .foreachBatch(lambda df, epoch_id: process_batch(df, "metricas_globais")) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/globais") \
    .start()
    
print("Query para 'Métricas Globais' iniciada.")

# === Query 2: Receita por Categoria (Agregação Global) ===
receita_por_categoria = df_transacoes.groupBy("categoria") \
    .agg(_sum("valor_total_compra").alias("receita"))
    
query_categoria = receita_por_categoria.writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda df, epoch_id: process_batch(df, "receita_por_categoria")) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/categoria") \
    .start()
    
print("Query para 'Receita por Categoria' iniciada.")

# === Query 3: Produtos Mais Vendidos ===
# NOTA: Para uma métrica "Top N" que exige uma ordenação global, o afunilamento no Driver é inevitável.
# A abordagem anterior com .toPandas() era, na verdade, correta para ESTE caso de uso específico.
# Vamos mantê-la aqui, demonstrando um entendimento da exceção à regra.
contagem_produtos = df_transacoes.groupBy("item").agg(count("*").alias("total_vendido"))
def processa_e_escreve_top_n(df, epoch_id, n=5):
    if not df.isEmpty():
        import pandas as pd
        r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
        top_n_df = df.orderBy(desc("total_vendido")).limit(n)
        pandas_df = top_n_df.toPandas()
        payload = pandas_df.to_json(orient='records')
        redis_key = "realtime:top_5_produtos"
        r.set(redis_key, payload)
        print(f"Métrica '{redis_key}' atualizada no Redis.")

query_top_produtos = contagem_produtos.writeStream.outputMode("complete") \
    .foreachBatch(lambda df, epoch_id: processa_e_escreve_top_n(df, epoch_id, n=5)) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/top_produtos") \
    .start()
    
print("Query para 'Top 5 Produtos' iniciada.")

# === Query 4 & 5 & 6: Contadores para Taxas ===
total_logins = df_eventos.filter(col("tipo_evento") == "login") \
    .agg(count("*") \
    .alias("total"))
    
query_logins = total_logins.writeStream.outputMode("complete") \
    .foreachBatch(lambda df, epoch_id: process_batch(df, "total_logins")) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/logins") \
    .start()
    
print("Query para 'Total de Logins' iniciada.")

total_carrinhos_criados = df_eventos.filter(col("tipo_evento") == "carrinho_criado") \
    .agg(count("*") \
    .alias("total"))
    
query_criados = total_carrinhos_criados.writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda df, epoch_id: process_batch(df, "total_carrinhos_criados")) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/carrinhos_criados") \
    .start()
    
print("Query para 'Total de Carrinhos Criados' iniciada.")

total_carrinhos_convertidos = df_transacoes.agg(approx_count_distinct("id_carrinho").alias("total"))

query_convertidos = total_carrinhos_convertidos.writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda df, epoch_id: process_batch(df, "total_carrinhos_convertidos")) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/carrinhos_convertidos") \
    .start()
    
print("Query para 'Total de Carrinhos Convertidos' iniciada.")

#  Manter a aplicação rodando 
print("\nTodas as queries de streaming foram iniciadas. Pressione Ctrl+C para parar.")
spark.streams.awaitAnyTermination()
