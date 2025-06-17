import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ArrayType

# --- 1. CONFIGURAÇÃO À PROVA DE FALHAS PARA WINDOWS ---
# Define o HADOOP_HOME diretamente no ambiente do script.
# Certifique-se de que este caminho está correto.
os.environ['HADOOP_HOME'] = 'C:\\hadoop'

# --- 2. Configuração da Sessão Spark ---
# A sessão Spark é o ponto de entrada para qualquer funcionalidade do Spark.
spark = SparkSession \
    .builder \
    .appName("RealTimeEcommerceDashboard") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .config("spark.driver.extraJavaOptions", f"-Dhadoop.home.dir={os.environ['HADOOP_HOME']}") \
    .config("spark.executor.extraJavaOptions", f"-Dhadoop.home.dir={os.environ['HADOOP_HOME']}") \
    .getOrCreate()

# Reduzir o nível de log para evitar poluir a saída
spark.sparkContext.setLogLevel("WARN")

print("========================= Sessão Spark iniciada com sucesso.")

# --- 3. Definição dos Schemas ---
# (Schemas permanecem os mesmos)
schema_transacoes = StructType([
    StructField("id_transacao", StringType(), True), StructField("id_pedido", StringType(), True),
    StructField("id_usuario", StringType(), True), StructField("nome_usuario", StringType(), True),
    StructField("id_produtos", ArrayType(StringType()), True), StructField("categorias_produtos", ArrayType(StringType()), True),
    StructField("quantidade_total_itens", IntegerType(), True), StructField("valor_total_compra", DoubleType(), True),
    StructField("data_compra", TimestampType(), True), StructField("metodo_pagamento", StringType(), True),
    StructField("status_pedido", StringType(), True), StructField("id_carrinho", StringType(), True)
])
schema_eventos = StructType([
    StructField("id_evento", StringType(), True), StructField("id_usuario", StringType(), True),
    StructField("id_sessao", StringType(), True), StructField("tipo_evento", StringType(), True),
    StructField("id_carrinho", StringType(), True), StructField("id_produto", StringType(), True),
    StructField("timestamp_evento", TimestampType(), True),
])

# --- 4. Leitura dos Streams do Kafka ---
# (Leitura dos streams permanece a mesma)
KAFKA_BROKER_URL = "localhost:9092"
df_transacoes_raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BROKER_URL).option("subscribe", "transacoes_vendas").option("startingOffsets", "latest").load()
df_eventos_raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BROKER_URL).option("subscribe", "eventos_web").option("startingOffsets", "latest").load()

# --- 5. Parsing e Estruturação dos DataFrames ---
# (Parsing permanece o mesmo)
df_transacoes = df_transacoes_raw.select(from_json(col("value").cast("string"), schema_transacoes).alias("data")).select("data.*").withWatermark("data_compra", "10 minutes")
df_eventos = df_eventos_raw.select(from_json(col("value").cast("string"), schema_eventos).alias("data")).select("data.*").withWatermark("timestamp_evento", "10 minutes")

print("========================= Streams do Kafka sendo processados e estruturados.")

# --- 6. Cálculo das Métricas em Tempo Real ---
vendas_por_minuto = df_transacoes \
    .groupBy(window(col("data_compra"), "1 minute")) \
    .agg(_sum("valor_total_compra").alias("receita_total")) \
    .select("window.start", "window.end", "receita_total")

# --- 7. Saída dos Resultados (Output) ---
# Adicionamos a opção de checkpointLocation para usar nossa pasta segura.
CHECKPOINT_PATH = "C:/tmp/spark_checkpoints"

query_vendas = vendas_por_minuto \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start()

print(" ========================= Query de streaming iniciada. Aguardando dados do Kafka...")
print(" ========================= Pressione Ctrl+C para parar.")

query_vendas.awaitTermination()
