from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from pathlib import Path

import sys
# db_dir = Path(__file__).resolve().parent.parent / 'db' # equivalente a ..\db
root_dir = Path(__file__).resolve().parent.parent
# print(f"db_dir: {db_dir}, {db_dir.exists()}")
# sys.path.append(str(db_dir))
sys.path.append(str(root_dir))
from db.db_config import DB_CONFIG, DB_NAME

# Lendo dados das tabelas
def read_from_postgres_with_partition(spark: SparkSession, jdbc_url: str, db_properties: dict, table_name: str, partition_column: str, num_partitions: int) -> DataFrame:
    """Lê uma tabela do PostgreSQL de forma paralela e dinâmica."""
    
    bounds_query = f"(SELECT MIN({partition_column}) as min_id, MAX({partition_column}) as max_id FROM {table_name}) as bounds"
    print(f"Buscando limites (min/max) da coluna '{partition_column}' na tabela '{table_name}'...")
    bounds_df = spark.read.jdbc(url=jdbc_url, table=bounds_query, properties=db_properties)
    bounds_row = bounds_df.first()

    if bounds_row and bounds_row["min_id"] is not None:
        lower_bound = bounds_row["min_id"]
        upper_bound = bounds_row["max_id"]

        print(f"Leitura paralela iniciada. Limites: {lower_bound} a {upper_bound}. Partições: {num_partitions}.")
        return spark.read.jdbc(
            url=jdbc_url, table=table_name, properties=db_properties,
            column=partition_column, lowerBound=lower_bound,
            upperBound=upper_bound, numPartitions=num_partitions
        )
    else:
        print(f"A tabela '{table_name}' está vazia. Realizando leitura simples.")
        return spark.read.jdbc(url=jdbc_url, table=table_name, properties=db_properties)

if __name__ == "__main___":
    # jdbc_driver_path = os.getenv("JDBC_JAR_PATH")
    # --- Lendo drive psql -> Java
    jdbc_driver_path = Path("C:/spark/jdbc/postgresql-42.7.7.jar")

    if jdbc_driver_path.exists():
        jdbc_driver_path = rf"{jdbc_driver_path}"
        print(jdbc_driver_path)

    #  --- Criando sessão Spark ---
    spark = SparkSession.builder \
        .appName("IngestaoPostgresParaDelta") \
        .config("spark.jars", jdbc_driver_path) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    print("Lendo a tabela 'transacoes_vendas' do PostgreSQL...")
    tabela_tv_nome = 'transacoes_vendas'
    coluna_id_tv = "id_transacao"
    tv_df_raw = read_from_postgres_with_partition(spark, coluna_id_tv, tabela_tv_nome)
    print("Leitura da tabela 'transacoes_vendas' concluída.")
    tv_df_raw.printSchema()

    print("Lendo a tabela 'dados_clientes' do PostgreSQL...")
    tabela_dc_nome = 'dados_clientes'
    coluna_id_dc = "id_usuario"
    dc_df_raw = read_from_postgres_with_partition(spark, coluna_id_dc, tabela_dc_nome)
    print("Leitura da tabela 'dados_clientes' concluída.")
    dc_df_raw.printSchema()

    print("Lendo a tabela 'eventos_web' do PostgreSQL...")
    tabela_ew_nome = 'eventos_web'
    coluna_id_dc = "id_evento"
    ew_df_raw = read_from_postgres_with_partition(spark, coluna_id_dc, tabela_ew_nome)
    print("Leitura da tabela 'eventos_web' concluída.")
    ew_df_raw.printSchema()

    print("Lendo a tabela 'catalogo_produtos' dos PostgreSQL...")
    tabela_cp_nome = 'catalogo_produtos'
    coluna_id_cp = "id_produto"
    cp_df_raw = read_from_postgres_with_partition(spark, coluna_id_cp, tabela_cp_nome)
    print("Leitura da tabela 'catalogo_produtos' concluída.")
    cp_df_raw.printSchema()