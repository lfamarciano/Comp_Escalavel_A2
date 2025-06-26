from pyspark.sql.dataframe import DataFrame
import redis

from typing import Any

import os
from dotenv import load_dotenv
load_dotenv()

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = os.environ.get("REDIS_PORT", "6379")

def connect_to_redis_and_publish(redis_key: str, data: Any):
    try:
        # Conectando ao redis
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        # Verificando conexão ativa
        r.ping()
        print("Conectado ao Redis com sucesso!")

        # Publicando dados
        r.set(redis_key, data)
        print(f"Dados publicados no Redis com sucesso na chave: '{redis_key}'")

    except redis.exceptions.ConnectionError as e:
        print(f"ERRO CRÍTICO: Não foi possível conectar ao Redis. Verifique se o container está rodando. Detalhes: {e}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado durante a publicação no Redis: {e}")

def publish_dataframe_to_redis(df: DataFrame, redis_key: str):
    """
    Converte um DataFrame Spark para uma string JSON e o publica em uma chave do Redis
    Função destinada para processamento em Batch, não em streaming
    """

    print(f"Coletando DataFrame para publicação no Redis na chave: {redis_key}")
    if df.isEmpty():
        raise ValueError("Dataframe vazio, nada para publicar")

    # Converting to pandas DataFrame
    pandas_df = df.toPandas()

    # Converting to json
    json_data = pandas_df.to_json(orient="records", date_format="iso")

    connect_to_redis_and_publish(redis_key, json_data)

def publish_metric_to_redis(metric: str | int | float, redis_key: str):
    """
    Publica uma métrica no formato str, int ou float no Redis.
    """
    
    print(f"Coletando métrica para publicação no Redis na chave: {redis_key}")
    if metric == None or not (isinstance(metric, (str, int, float))):
        raise TypeError(f"Métrica inválida para publicação: {metric}")

    connect_to_redis_and_publish(redis_key, metric)