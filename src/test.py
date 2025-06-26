import redis
from config import (
    POSTGRES_PASSWORD,
    POSTGRES_DATABASE,
    POSTGRES_USER,
    POSTGRES_HOST,
    POSTGRES_PORT,
    REDIS_HOST,
    REDIS_PORT
)
def get_redis_connection():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        r.ping()
        print(f"Conexão com Redis ({REDIS_HOST}) estabelecida/reutilizada.")
        return r
    except redis.exceptions.ConnectionError as e:
        print(f"Não foi possível conectar ao Redis em '{REDIS_HOST}'. Detalhes: {e}")
        return None
    
get_redis_connection()