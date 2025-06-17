import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

DB_CONFIG = {
    'host': 'localhost',
    'user': 'postgres',
    'password': '123',
    'port': '5432'
}

DB_NAME = 'ecommerce_db'

def create_database():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
            exists = cursor.fetchone()
            
            if not exists:
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
                print(f"Database '{DB_NAME}' criado com sucesso!")
            else:
                print(f"ℹDatabase '{DB_NAME}' já existe. Prosseguindo...")
                
    except Exception as e:
        print(f"Erro ao criar database: {e}")
    finally:
        if conn is not None:
            conn.close()

def create_tables():
    conn = None
    try:
        # Conecta ao novo database
        conn_config = {**DB_CONFIG, 'database': DB_NAME}
        conn = psycopg2.connect(**conn_config)
        
        commands = (
            """
            CREATE TABLE IF NOT EXISTS dados_clientes (
                id_usuario SERIAL PRIMARY KEY,
                nome_usuario VARCHAR(100) NOT NULL,
                email_usuario VARCHAR(100) UNIQUE NOT NULL,
                data_cadastro DATE NOT NULL,
                segmento_cliente VARCHAR(50),
                cidade VARCHAR(50),
                estado VARCHAR(2),
                pais VARCHAR(30) DEFAULT 'Brasil'
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS catalogo_produtos (
                id_produto SERIAL PRIMARY KEY,
                nome_produto VARCHAR(100) NOT NULL,
                descricao_produto TEXT,
                categoria VARCHAR(50) NOT NULL,
                preco_unitario NUMERIC(10,2) NOT NULL,
                estoque_disponivel INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS transacoes_vendas (
                id_transacao SERIAL PRIMARY KEY,
                id_pedido VARCHAR(20) NOT NULL,
                id_usuario INTEGER REFERENCES dados_clientes(id_usuario),
                id_produto INTEGER REFERENCES catalogo_produtos(id_produto),
                quantidade_produto INTEGER NOT NULL,
                valor_total_compra NUMERIC(10,2) NOT NULL,
                data_compra TIMESTAMP NOT NULL,
                metodo_pagamento VARCHAR(20) NOT NULL,
                status_pedido VARCHAR(20) NOT NULL,
                id_carrinho VARCHAR(20) NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS eventos_web (
                id_evento SERIAL PRIMARY KEY,
                id_usuario INTEGER REFERENCES dados_clientes(id_usuario),
                id_sessao VARCHAR(50) NOT NULL,
                id_carrinho VARCHAR(20),
                tipo_evento VARCHAR(50) NOT NULL,
                id_produto INTEGER REFERENCES catalogo_produtos(id_produto),
                timestamp_evento TIMESTAMP NOT NULL
            )
            """
        )
        
        with conn.cursor() as cursor:
            for command in commands:
                cursor.execute(command)
            conn.commit()
        print("Tabelas criadas com sucesso!")
        
    except Exception as e:
        print(f"Erro ao criar tabelas: {e}")
        if conn is not None:
            conn.rollback()
    finally:
        if conn is not None:
            conn.close()

def main():
    print("Iniciando criação do banco de dados...")
    create_database()
    create_tables()
    print("Processo concluído!")

if __name__ == "__main__":
    main()
