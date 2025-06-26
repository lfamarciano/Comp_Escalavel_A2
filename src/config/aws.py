# Arquivo para definir os endereços para os serviços na AWS.
import os

KAFKA_HOST = os.environ.get('KAFKA_HOST', '3.90.34.101:9092')
TRANSACTIONS_TOPIC = 'transacoes_vendas'
WEB_EVENTS_TOPIC = 'eventos_web'

REDIS_HOST = os.environ.get('REDIS_HOST', '54.235.3.149')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')

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

POSTGRES_HOST = DB_HOST
POSTGRES_USER = DB_USER
POSTGRES_PASSWORD = DB_PASSWORD
POSTGRES_PORT = '5432'
POSTGRES_DATABASE = DB_NAME

OUT_URL = DB_HOST

# ... etc.