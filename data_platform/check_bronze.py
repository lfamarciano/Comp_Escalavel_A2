# 2_data_platform/check_bronze.py

from pyspark.sql import SparkSession

# O caminho para a sua tabela Bronze, conforme definido no config.yaml
BRONZE_TABLE_PATH = "data/bronze/transactions"

print(f"Lendo a tabela Delta em: {BRONZE_TABLE_PATH}")

spark = (
    SparkSession.builder.appName("BronzeChecker")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

try:
    # Lê a tabela Delta como um DataFrame estático
    bronze_df = spark.read.format("delta").load(BRONZE_TABLE_PATH)

    print("\n--- Schema da Tabela Bronze ---")
    bronze_df.printSchema()

    print("\n--- Contagem de Registros Ingeridos ---")
    count = bronze_df.count()
    print(f"Total de registros: {count}")


    print("\n--- Amostra dos Dados na Camada Bronze ---")
    bronze_df.show(5, truncate=False)

except Exception as e:
    print(f"Erro ao ler a tabela. O job de ingestão já processou algum lote? Erro: {e}")