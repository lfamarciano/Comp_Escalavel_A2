# Implementa um worker que fica, a cada pequeno intervalo, lendo o postgres
# em busca de atulizações nos dados e escrevendo essas atualizações na 
# camada bronze do DeltaLake.

# Rode com:
# spark-submit --packages io.delta:delta-spark_2.13:4.0.0 --jars C:\spark\jdbc\postgresql-42.7.7.jar src\historical-pipeline\ingestion_worker.py --conf spark.driver.log.level=WARN"

# incremental_bronze_worker.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
import signal

# Supondo que db_config.py exista em um diretório 'db'
# Adicionando configurações
import sys
from pathlib import Path
import threading

import os


from config import POSTGRES_PASSWORD, POSTGRES_DATABASE, POSTGRES_USER, POSTGRES_HOST, POSTGRES_PORT, JDBC_DRIVER_PATH

DELTALAKE_BASE_PATH = os.environ.get("DELTALAKE_BASE_PATH", "app/deltalake")

def get_max_id_from_delta(spark: SparkSession, delta_path: str, id_column: str) -> int:
    """Busca o ID máximo de uma tabela Delta. Retorna 0 se a tabela não existir ou estiver vazia."""
    print(f"Verificando o ID máximo na tabela Delta: {delta_path}")
    try:
        max_id_row = spark.read.format("delta").load(delta_path).select(F.max(id_column)).first()
        if max_id_row and max_id_row[0] is not None:
            max_id = max_id_row[0]
            print(f"ID máximo encontrado: {max_id}")
            return max_id
        else:
            print("Tabela Delta vazia ou sem ID máximo. Começando do zero.")
            return 0
    except AnalysisException as e:
        # Erro comum se o caminho não existir na primeira execução
        if "Path does not exist" in str(e):
            print("Caminho da tabela Delta não existe ainda. Será criado. Começando do zero.")
            return 0
        else:
            raise e

def update_table_deltalake(spark: SparkSession, jdbc_url: str, table_name: str, id_column: str, db_properties: dict, ingestion_interval_seconds: int, shutdown_event: threading.Event):
    """
    Função do worker que continuamente busca novos dados de uma tabela e os adiciona ao Delta Lake.
    O loop continua enquanto o evento 'shutdown_event' não for acionado.
    """
    # bronze_path = str(Path(f"deltalake/bronze/{table_name}")) # local
    bronze_path = f"{DELTALAKE_BASE_PATH}/bronze/{table_name}"

    # Path(bronze_path).mkdir(exist_ok=True, parents=True)

    # Passo 3: O loop agora verifica o evento de parada.
    while not shutdown_event.is_set():
        try:
            max_id_in_delta = get_max_id_from_delta(spark, bronze_path, id_column)
            
            query = f"(SELECT * FROM {table_name} WHERE {id_column} > {max_id_in_delta}) as new_data"
            
            print(f"[{table_name}] Buscando novos registros com ID > {max_id_in_delta}...")
            new_data_df = spark.read.jdbc(url=jdbc_url, table=query, properties=db_properties)
            
            count = new_data_df.count()
            if count > 0:
                print(f"[{table_name}] Encontrados {count} novos registros.")
                new_data_df.write.format("delta").mode("append").save(bronze_path)
                print(f"[{table_name}] Novos dados adicionados à camada Bronze com sucesso.")
            else:
                print(f"[{table_name}] Nenhum registro novo encontrado.")
            
        except Exception as e:
            print(f"[{table_name}] ERRO durante o ciclo de ingestão: {e}")
            print(f"[{table_name}] Continuando para o próximo ciclo após o intervalo de espera.")
        
        # A espera agora usa o evento para despertar mais cedo se necessário.
        # shutdown_event.wait() retorna True se o evento for acionado, False se o timeout expirar.
        print(f"[{table_name}] Aguardando {ingestion_interval_seconds} segundos...")
        shutdown_event.wait(timeout=ingestion_interval_seconds)

    print(f"--- Worker [{table_name}] finalizando. ---")

def main():
    """
    Worker de longa duração que ingere dados incrementalmente do PostgreSQL para o Delta Lake.
    """
    # --- Configurando jdbc ---
    # jdbc_driver_path = Path("C:/spark/jdbc/postgresql-42.7.7.jar") # local
    jdbc_driver_path = Path(JDBC_DRIVER_PATH)
    if not jdbc_driver_path.exists():
        raise FileNotFoundError(f"Arquivo do driver JDBC não foi encontrado em: {jdbc_driver_path}")
    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"

    #  --- Criando sessão Spark ---
    ingestion_interval_seconds = 5  # Intervalo entre as execuções
    
    # --- Sinal de parada para os workers ---
    shutdown_event = threading.Event()
    def signal_handler(sig, frame):
        print("\nSinal de interrupção (Ctrl+C) recebido. Enviando sinal de parada para os workers...")
        shutdown_event.set()

    # Registrnado manipulador para o sinal de interrupção (SIGINT)
    signal.signal(signal.SIGINT, signal_handler)

    # --- Configurando variáveis ---
    db_properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

    # Criando threads e eventos de desligamento
    shutdown_event = threading.Event()
    threads = []

    # Tabelas a serem processadas e seus ids
    tables_to_process = {
        "transacoes_vendas": "id_transacao",
        "eventos_web": "id_evento"
    }
    
    # --- Iniciando sessão spark ---
    spark = SparkSession.builder \
        .appName("ContinuousIngestionWorker") \
        .config("spark.jars", jdbc_driver_path) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    print("--- Worker de Ingestão Contínua Iniciado ---")
    print(f"Verificando novos dados a cada {ingestion_interval_seconds} segundos. Pressione Ctrl+C para parar.")
    
    print("--- Iniciando Workers de Ingestão Contínua ---")

    for table, id_col in tables_to_process.items():
        # Criando, registrando e iniciando thread
        thread = threading.Thread(
            target=update_table_deltalake,
            args=(
                spark,jdbc_url,table,id_col,db_properties,ingestion_interval_seconds,shutdown_event
            ),
            daemon=True
        )
        threads.append(thread)
        thread.start()
        print(f"Worker para a tabela '{table}' iniciado")
    
    # Esperando pelo sinal para encerrar
    shutdown_event.wait()

    # Finalizando threads e thread principal
    print("Aguardando finalização dos workers...")
    # Finalizando threads
    for thread in threads:
        thread.join()
    
    # Garante que a sessão Spark seja encerrada de forma limpa
    print("Encerrando a sessão Spark...")
    spark.stop()
    print("--- Workers de Ingestão Finalizados ---")
    
if __name__ == "__main__":
    main()