from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import redis

import json
from pathlib import Path
import time

import redis.exceptions

# ... (funções do redis) ...

def publish_dataframe_to_redis(df: DataFrame, redis_key: str):
    """
    Converte um DataFrame Spark para uma string JSON e o publica em uma chave do Redis
    Está função para 'batch', não para streaming
    """

    print(f"Coletando DataFrame para publica no Redis na chave: {redis_key}")
    if df.isEmpty():
        print("DataFrame está vazio. Nada para publicar")

    # Converting to pandas DataFrame
    pandas_df = df.toPandas()

    # Converting to json
    json_data = pandas_df.to_json(orient="records", date_format="iso")

    try:
        # Conectando ao redis
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        # Verificando conexão ativa
        r.ping()
        print("Conectado ao Redis com sucesso!")

        # Publicando JSON
        r.set(redis_key, json_data)
        print(f"Dados publicados no Redis com sucesso na chave: '{redis_key}'")

    except redis.exceptions.ConnectionError as e:
        print(f"ERRO CRÍTICO: Não foi possível conectar ao Redis. Verifique se o container está rodando. Detalhes: {e}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado durante a publicação no Redis: {e}")


def main():
    spark = SparkSession.builder \
        .appName("RedisPublisher") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Lendo dados simulados do parquet
    simulated_gold_path = Path("data/simulated_gold/receita_diaria.parquet")
    print(30 * "*")
    print(f"Lendo dados da camada Ouro simulada de: {simulated_gold_path}")
    time.sleep(0.5)
    
    if not simulated_gold_path.exists():
        raise FileNotFoundError(
            f"Arquivo de simulação não encontrado em '{simulated_gold_path}'. "
            "Execute o pipeline principal (run_pipeline.py) na branch main primeiro."
        )
        
    gold_metrics_df = spark.read.parquet(str(simulated_gold_path))
    
    print(30 * "*")
    print("Dados simulados carregados com sucesso:")
    time.sleep(0.5)
    gold_metrics_df.show(5)
    
    # 2. PUBLICAR O DATAFRAME NO REDIS
    # Usamos um nome de chave claro para separar dos dados de tempo real.
    redis_key_name = "historical:daily_revenue_metrics"
    publish_dataframe_to_redis(gold_metrics_df, redis_key_name)
    
    print(30 * "*")
    print("\nProcesso de publicação concluído.")
    time.sleep(0.5)
    spark.stop()

if __name__ == "__main__":
    main()