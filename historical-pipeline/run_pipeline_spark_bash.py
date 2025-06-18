from pyspark.sql import SparkSession
from dotenv import load_dotenv

from pathlib import Path
import os

# Importando todas as funções
import etl_jobs

# Impotando db_config com as configurações do db de db_import
from db_import import DB_CONFIG, DB_NAME

def main():
    """Função principal que orquestra todo o pipeline de ETL."""
    
    # --- 1. CONFIGURAÇÃO E CRIAÇÃO DA SESSÃO SPARK ---
    jdbc_driver_path = os.getenv("JDBC_JAR_PATH")
    # Se tivermos lido corretamente a variável de ambiente
    if jdbc_driver_path:
        jdbc_driver_path = Path(jdbc_driver_path)
    else: # fallback que segue exemplo no readme
        jdbc_driver_path = Path("C:/spark/jdbc/postgresql-42.7.7.jar")

    if jdbc_driver_path.exists():
        jdbc_driver_path = rf"{jdbc_driver_path}"
        print(jdbc_driver_path)
    else:
        raise FileNotFoundError(F"JDBC Driver not found at: {jdbc_driver_path}")

    spark = SparkSession.builder \
        .appName("PipelineETL-PostgresParaDelta") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()

    jdbc_url = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_NAME}"
    db_properties = {
        "user": DB_CONFIG['user'],
        "password": DB_CONFIG['password'],
        "driver": "org.postgresql.Driver"
    }

    # --- 2. EXECUÇÃO DO PIPELINE ---
    # Ler transações
    print("Iniciando leitura da tabela de transações...")
    tv_raw_df = etl_jobs.read_from_postgres_with_partition(spark, jdbc_url, db_properties, 'transacoes_vendas', 'id_transacao')
    
    # Ler clientes
    print("\nIniciando leitura da tabela de clientes...")
    dc_raw_df = etl_jobs.read_from_postgres_with_partition(spark, jdbc_url, db_properties, 'dados_clientes', 'id_usuario')

    # Ler eventos web
    print("\nIniciando leitura da tabela de eventos web...")
    ew_raw_df = etl_jobs.read_from_postgres_with_partition(spark, jdbc_url, db_properties, 'eventos_web', 'id_evento')

    # Ler catálogo de produtos
    print("\nIniciando leitura da tabela de catálogo de produtos...")
    cp_raw_df = etl_jobs.read_from_postgres_with_partition(spark, jdbc_url, db_properties, 'catalogo_produtos', 'id_produto')

    # Limpar dados
    print("\nIniciando limpeza dos dados...")
    tv_cleaned_df = etl_jobs.clean_transactions_data(tv_raw_df)
    dc_cleaned_df = etl_jobs.clean_clients_data(dc_raw_df)
    ew_cleaned_df = etl_jobs.clean_web_events_data(ew_raw_df)
    cp_cleaned_df = etl_jobs.clean_products_data(cp_raw_df)

    print("\nSchemas após limpeza:")
    tv_cleaned_df.printSchema()
    dc_cleaned_df.printSchema()
    ew_cleaned_df.printSchema()
    cp_cleaned_df.printSchema()
    
    # Escrever para o Delta Lake
    delta_base_path = etl_jobs.deltalake_bronze_path
    delta_base_path.mkdir(parents=True, exist_ok=True) # Garante que o diretório exista

    print("\nIniciando escrita para o Delta Lake...")
    etl_jobs.write_to_delta(tv_cleaned_df, str(delta_base_path / "transacoes_vendas"))
    etl_jobs.write_to_delta(dc_cleaned_df, str(delta_base_path / "dados_clientes"))
    etl_jobs.write_to_delta(ew_cleaned_df, str(delta_base_path / "eventos_web"))
    etl_jobs.write_to_delta(cp_cleaned_df, str(delta_base_path / "catalogo_produtos"))

    print("\nPipeline concluído com sucesso!")
    spark.stop()

if __name__ == "__main__":
    main()