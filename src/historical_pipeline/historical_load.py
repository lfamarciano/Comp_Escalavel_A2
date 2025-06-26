# historical_pipeline/historical_load.py
import os
from pyspark.sql import SparkSession

from dotenv import load_dotenv
load_dotenv()
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config import POSTGRES_PASSWORD, POSTGRES_DATABASE, POSTGRES_USER, POSTGRES_HOST, POSTGRES_PORT
# POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "123")
# POSTGRES_DATABASE = os.environ.get("POSTGRES_DATABASE", "ecommerce_db")
# POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
# POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
# POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")

def main():
    spark = SparkSession.builder.appName("HistoricalLoad") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    db_properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }
    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"
    
    base_path = os.environ.get("DELTALAKE_BASE_PATH")
    if not base_path:
        raise ValueError("DELTALAKE_BASE_PATH environment variable not set!")

    tables = ["transacoes_vendas", "eventos_web", "dados_clientes", "catalogo_produtos"]

    print("--- Starting Full Historical Load ---")
    for table in tables:
        print(f"Processing table: {table}...")
        
        # 1. Read the ENTIRE table from PostgreSQL
        df = spark.read.jdbc(url=jdbc_url, table=table, properties=db_properties)
        
        # 2. OVERWRITE the Delta table with the full dataset
        delta_path = f"{base_path}/bronze/{table}"
        print(f"Writing {df.count()} rows to {delta_path} in overwrite mode.")
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_path)
        
        print(f"Table {table} loaded successfully.")

    print("--- Full Historical Load Complete ---")
    spark.stop()

if __name__ == "__main__":
    main()