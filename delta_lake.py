from dotenv import load_dotenv
from pyspark.sql import SparkSession

from pathlib import Path
import os

import sys
db_dir = Path(__file__).resolve().parent / 'db' # equivalente a .\db
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
def read_table_partitioning(partition_column: str, table_name: str, num_partitions: int = 5):
    # 1. CRIAR A QUERY DE METADADOS PARA BUSCAR OS LIMITES (MIN/MAX)
    bounds_query = f"(SELECT MIN({partition_column}) as min_id, MAX({partition_column}) as max_id FROM {table_name}) as bounds"

    print(f"Buscando limites (min/max) da coluna '{partition_column}' na tabela '{table_name}'...")

    # Executa a query de limites. O resultado será um DataFrame com apenas 1 linha.
    bounds_df = spark.read.jdbc(url=jdbc_url, table=bounds_query, properties=db_properties)

    # Coleta o resultado para o driver do Spark.
    # .first() é seguro aqui, pois a query sempre retornará 1 linha.
    bounds_row = bounds_df.first()

    # 2. VERIFICAR SE A TABELA NÃO ESTÁ VAZIA E REALIZAR A LEITURA PRINCIPAL
    if bounds_row and bounds_row["min_id"] is not None:
        lower_bound = bounds_row["min_id"]
        upper_bound = bounds_row["max_id"]

        print(f"Leitura paralela iniciada. Limites: {lower_bound} a {upper_bound}. Partições: {num_partitions}.")

        # Usa os limites dinâmicos na leitura JDBC principal
        return spark.read.jdbc(
            url=jdbc_url,
            table=table_name,
            properties=db_properties,
            column=partition_column,
            lowerBound=lower_bound,
            upperBound=upper_bound,
            numPartitions=num_partitions
        )
    else:
        # Caso a tabela esteja vazia, a query de min/max retornará NULL.
        # Neste caso, fazemos uma leitura simples (não paralela) que resultará
        # em um DataFrame vazio, sem causar erros.
        print(f"A tabela '{table_name}' está vazia ou não foi possível determinar os limites. Realizando leitura simples.")
        return spark.read.jdbc(
            url=jdbc_url,
            table=table_name,
            properties=db_properties
        )


print("Lendo a tabela 'transacoes_vendas' do PostgreSQL...")
tabela_tv_nome = 'transacoes_vendas'
coluna_id_tv = "id_transacao"
tv_df_raw = read_table_partitioning(coluna_id_tv, tabela_tv_nome)
print("Leitura da tabela 'transacoes_vendas' concluída.")
tv_df_raw.printSchema()
# tv_df_raw = spark.read.jdbc(
#     url=jdbc_url,
#     table="transacoes_vendas", # Nome da tabela no banco
#     properties=db_properties
# )

print("Lendo a tabela 'dados_clientes' do PostgreSQL...")
tabela_dc_nome = 'dados_clientes'
coluna_id_dc = "id_usuario"
dc_df_raw = read_table_partitioning(coluna_id_dc, tabela_dc_nome)
print("Leitura da tabela 'dados_clientes' concluída.")
dc_df_raw.printSchema()
# dc_df_raw = spark.read.jdbc(
#     url=jdbc_url,
#     table="dados_clientes", # Nome da tabela no banco
#     properties=db_properties
# )
