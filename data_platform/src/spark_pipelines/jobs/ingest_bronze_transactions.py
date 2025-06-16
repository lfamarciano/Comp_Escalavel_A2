# 2_data_platform/src/spark_pipelines/jobs/1_ingest_bronze_transactions.py

import yaml
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Importando nosso construtor de sessão Spark
from ..utils.spark_builder import SparkBuilder

def main():
    """
    Job principal que lê dados de transações do Kafka e os ingere na camada Bronze.
    """
    print("Iniciando o job de ingestão da camada Bronze para Transações...")

    # Carregar configurações do arquivo YAML
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    kafka_config = config['kafka']
    delta_config = config['delta_paths']
    checkpoint_config = config['spark_checkpoints']

    # 1. Construir a Sessão Spark
    spark = SparkBuilder("BronzeIngestionTransactions").build()

    # 2. Definir o Schema do JSON
    # Este schema deve corresponder exatamente à estrutura do JSON enviado pelo seu producer.py
    schema = StructType([
        StructField("id_transacao", StringType(), True),
        StructField("id_pedido", StringType(), True),
        StructField("id_usuario", IntegerType(), True),
        StructField("nome_usuario", StringType(), True),
        StructField("id_produto", IntegerType(), True),
        StructField("categoria", StringType(), True),
        StructField("item", StringType(), True),
        StructField("valor_total_compra", DoubleType(), True),
        StructField("quantidade_produto", IntegerType(), True),
        StructField("data_compra", StringType(), True), # Lendo como string por enquanto
        StructField("metodo_pagamento", StringType(), True),
        StructField("status_pedido", StringType(), True),
        StructField("id_carrinho", StringType(), True),
    ])

    # 3. Ler o Stream do Kafka
    print(f"Lendo do tópico Kafka: {kafka_config['transactions_topic']}")
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'])
        .option("subscribe", kafka_config['transactions_topic'])
        .option("startingOffsets", "earliest") # Começa a ler desde o início do tópico
        .load()
    )

    # 4. Transformar os dados
    # Converter a coluna 'value' (que está em binário) para string, e então para JSON
    transformed_df = (
        kafka_df.select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn("ingestion_timestamp", current_timestamp()) # Adiciona metadado de ingestão
    )

    # 5. Escrever o Stream na Tabela Delta (Camada Bronze)
    bronze_path = delta_config['bronze_transactions']
    checkpoint_path = checkpoint_config['bronze_transactions']
    
    print(f"Escrevendo dados no formato Delta para: {bronze_path}")
    query = (
        transformed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path) # Essencial para resiliência
        .start(bronze_path)
    )

    print("Job em execução. Aguardando novos dados...")
    query.awaitTermination()

if __name__ == "__main__":
    main()