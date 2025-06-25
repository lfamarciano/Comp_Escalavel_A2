# Arquivo para definir os endereços para os serviços na AWS.
KAFKA_BROKER_URL = 'b-1.seu-cluster-msk...' # Endpoint do Amazon MSK
REDIS_HOST = 'seu-cluster-redis.xxxx...'  # Endpoint do ElastiCache
DB_CONFIG = {
    'host': 'postgres.chvwsyfmunoi.us-east-1.rds.amazonaws.com', # <== COLE O ENDPOINT AQUI
    'user': 'postgres', # O utilizador que você definiu ao criar o RDS
    'password': 123, # A senha que você definiu
    'port': '5432',
    'database': 'ecommerce_db'
}
DB_NAME = 'ecommerce_db'
# ... etc.