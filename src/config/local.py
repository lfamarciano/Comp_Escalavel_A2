# Arquivo para definir os endereços para o ambiente Docker local
import os
import json

OUT_URL = 'localhost'

REDIS_HOST = 'redis'
REDIS_PORT = '6379'

KAFKA_HOST= 'kafka:9092'
TRANSACTIONS_TOPIC = 'transacoes_vendas'
WEB_EVENTS_TOPIC = 'eventos_web'

POSTGRES_HOST = 'postgres'
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = '123'
POSTGRES_PORT = '5432'
POSTGRES_DATABASE = 'ecommerce_db'

dotenv_dbconfig = os.getenv("DB_CONFIG")

DB_HOST = os.environ.get('DB_HOST', 'postgres')

DB_CONFIG = json.loads(dotenv_dbconfig) if dotenv_dbconfig else {
    'host': 'postgres',
    'user': 'postgres',
    'password': '123',
    'port': '5432',
    'database': 'ecommerce_db'
}
DB_NAME = DB_CONFIG['database'] if dotenv_dbconfig else 'ecommerce_db'