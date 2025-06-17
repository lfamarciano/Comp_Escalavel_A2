from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ArrayType

# Configuração da Sessão Spark
spark = SparkSession \
    .builder \
    .appName("RealTimeEcommerceDashboard") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0")\
    .getOrCreate()

# Reduzir o nível de log para evitar poluir a saída com informações de INFO
spark.sparkContext.setLogLevel("WARN")

print("Sessão Spark iniciada com sucesso.")

# Definição dos Schemas
# Os schemas garantem que o Spark interprete corretamente o JSON vindo do Kafka.
# Eles devem corresponder EXATAMENTE à estrutura dos dados enviados pelo producer.py.

schema_transacoes = StructType([
    StructField("id_transacao", StringType(), True),
    StructField("id_pedido", StringType(), True),
    StructField("id_usuario", StringType(), True),
    StructField("nome_usuario", StringType(), True),
    StructField("id_produtos", ArrayType(StringType()), True),
    StructField("categorias_produtos", ArrayType(StringType()), True),
    StructField("quantidade_total_itens", IntegerType(), True),
    StructField("valor_total_compra", DoubleType(), True),
    StructField("data_compra", TimestampType(), True),
    StructField("metodo_pagamento", StringType(), True),
    StructField("status_pedido", StringType(), True),
    StructField("id_carrinho", StringType(), True)
])

schema_eventos = StructType([
    StructField("id_evento", StringType(), True),
    StructField("id_usuario", StringType(), True),
    StructField("id_sessao", StringType(), True),
    StructField("tipo_evento", StringType(), True),
    StructField("id_carrinho", StringType(), True),
    StructField("id_produto", StringType(), True),
    StructField("timestamp_evento", TimestampType(), True),
])

# --- 3. Leitura dos Streams do Kafka ---
# Conectando aos tópicos do Kafka. O Spark tratará esses tópicos como tabelas de dados contínuos.

KAFKA_BROKER_URL = "localhost:9092"

# Leitura do tópico de transações
df_transacoes_raw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER_URL) \
    .option("subscribe", "transacoes_vendas") \
    .option("startingOffsets", "latest") \
    .load()

# Leitura do tópico de eventos web
df_eventos_raw = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER_URL) \
    .option("subscribe", "eventos_web") \
    .option("startingOffsets", "latest") \
    .load()

# --- 4. Parsing e Estruturação dos DataFrames ---
# Os dados do Kafka vêm em formato binário. Precisamos convertê-los para string
# e depois aplicar nosso schema para obter um DataFrame estruturado.

# Processando as transações
df_transacoes = df_transacoes_raw \
    .select(from_json(col("value").cast("string"), schema_transacoes).alias("data")) \
    .select("data.*") \
    .withWatermark("data_compra", "10 minutes") # Permite dados com até 10 minutos de atraso

# Processando os eventos
df_eventos = df_eventos_raw \
    .select(from_json(col("value").cast("string"), schema_eventos).alias("data")) \
    .select("data.*") \
    .withWatermark("timestamp_evento", "10 minutes") # Permite dados com até 10 minutos de atraso

print("Streams do Kafka sendo processados e estruturados.")

# --- 5. Cálculo das Métricas em Tempo Real (Exemplo Inicial) ---
# Aqui começa a lógica de negócio. Vamos começar com a receita total em janelas de tempo.

# Métrica: Vendas totais por minuto
vendas_por_minuto = df_transacoes \
    .groupBy(
        window(col("data_compra"), "1 minute") # Agrupa os dados em janelas de 1 minuto
    ) \
    .agg(
        _sum("valor_total_compra").alias("receita_total") # Soma o valor das compras na janela
    ) \
    .select("window.start", "window.end", "receita_total")

# --- 6. Saída dos Resultados (Output) ---
# Por enquanto, vamos imprimir os resultados no console para verificar a lógica.
# O modo "update" mostra apenas as janelas que foram atualizadas no último batch.

query_vendas = vendas_por_minuto \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("Query de streaming iniciada. Aguardando dados do Kafka...")
print("Pressione Ctrl+C para parar.")

# Mantém a aplicação rodando enquanto as queries estiverem ativas
query_vendas.awaitTermination()

