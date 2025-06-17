from dotenv import load_dotenv
from pyspark.sql import SparkSession

from pathlib import Path
import os

import sys
db_dir = Path(__file__).resolve().parent / 'db'
sys.path.append(str(db_dir))
from db.db_config import DB_CONFIG, DB_NAME

load_dotenv()

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

# --- Lendo da base de dados ---
# Construindo a url JDBC no formato esperado pelo Spark
# Formato: jdbc:postgresql://<host>:<port>/<database>
jdbc_url = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_NAME}"

# Preparando dicionário para conexão
db_properties = {
    "user": DB_CONFIG['user'],
    "password": DB_CONFIG['password'],
    "driver": "org.postgresql.Driver"
}

# Lendo dados das tabelas
print("Lendo a tabela 'transacoes_vendas' do PostgreSQL...")
tv_df_raw = spark.read.jdbc(
    url=jdbc_url,
    table="transacoes_vendas", # Nome da tabela no banco
    properties=db_properties
)

print("Lendo a tabela 'dados_clientes' do PostgreSQL...")
dc_df_raw = spark.read.jdbc(
    url=jdbc_url,
    table="dados_clientes", # Nome da tabela no banco
    properties=db_properties
)

print("Leitura concluída! Exibindo o schema dos DataFrames:")