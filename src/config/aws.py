# Arquivo para definir os endereços para os serviços na AWS.
import os


KAFKA_HOST = '10.0.155.193:9092' # Endpoint do Amazon MSK
REDIS_HOST = 'seu-cluster-redis.xxxx...'  # Endpoint do ElastiCache

DB_HOST = os.environ.get('DB_HOST', 'postgres-identifier.chvwsyfmunoi.us-east-1.rds.amazonaws.com')
DB_USER = os.environ.get('DB_USER', 'postgres')
DB_PASSWORD = os.environ.get('DB_PASSWORD', "12345678")

DB_CONFIG = {
    'host': DB_HOST,
    'user': DB_USER, # O utilizador que você definiu ao criar o RDS
    'password': DB_PASSWORD, # A senha que você definiu no AWS RDS
    'port': '5432',
    'database': 'ecommerce_db'
}
DB_NAME = 'ecommerce_db'
# ... etc.