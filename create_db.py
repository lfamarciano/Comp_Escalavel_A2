import psycopg2
from psycopg2 import sql

# Database Settings
DB_CONFIG = {
    'host': 'localhost',
    'database': 'ecommerce_db',
    'user': 'postgres',
    'password': '123',
    'port': '5432'
}

def create_tables(conn):
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
    
    try:
        with conn.cursor() as cursor:
            for command in commands:
                cursor.execute(command)
            conn.commit()
        print("Tabelas criadas com sucesso!")
    except Exception as e:
        print(f"Erro ao criar tabelas: {e}")
        conn.rollback()

def main():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        create_tables(conn)
    except Exception as e:
        print(f"Erro de conexão: {e}")
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    main()