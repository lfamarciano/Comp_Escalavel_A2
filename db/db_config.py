from dotenv import load_dotenv
import os
import json

load_dotenv()

dotenv_dbconfig = os.getenv("DB_CONFIG")

DB_CONFIG = json.loads(dotenv_dbconfig) if dotenv_dbconfig else {
    'host': 'localhost',
    'user': 'postgres',
    'password': '123',
    'port': '5432',
    'database': 'ecommerce_db'
} # fallback to default configuration

DB_NAME = DB_CONFIG['database'] if dotenv_dbconfig else 'ecommerce_db'