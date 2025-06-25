import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from config import DB_CONFIG, DB_NAME

def create_database():
    """
    Garante um banco de dados limpo. Se o banco de dados já existir,
    ele o apaga e cria novamente de forma segura.
    Retorna True se bem-sucedido, False caso contrário.
    """
    # Conecta-se à base de dados de manutenção 'postgres' para realizar operações de admin
    conn_config_admin = {**DB_CONFIG, 'database': 'postgres'}
    conn = None
    try:
        conn = psycopg2.connect(**conn_config_admin)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        with conn.cursor() as cursor:
            # Verifica se a base de dados alvo existe
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
            exists = cursor.fetchone()
            
            # Se existir, apaga-a primeiro
            if exists:
                print(f"Database '{DB_NAME}' já existe. Apagando...")
                # Termina quaisquer outras conexões ativas com a base de dados alvo
                cursor.execute(sql.SQL(
                    "SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = %s"
                ), [DB_NAME])
                
                # Apaga a base de dados
                cursor.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(DB_NAME)))
                print(f"Database '{DB_NAME}' apagado com sucesso.")

            # Cria a base de dados do zero
            print(f"Criando database '{DB_NAME}'...")
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
            print(f"Database '{DB_NAME}' criado com sucesso!")
            return True # Retorna sucesso
            
    except psycopg2.Error as e:
        print(f"ERRO ao recriar o banco de dados: {e}")
        return False # Retorna falha
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
                id_pedido UUID NOT NULL,
                id_usuario INTEGER REFERENCES dados_clientes(id_usuario),
                id_produto INTEGER REFERENCES catalogo_produtos(id_produto),
                quantidade_produto INTEGER NOT NULL,
                valor_total_compra NUMERIC(10,2) NOT NULL,
                data_compra TIMESTAMP NOT NULL,
                metodo_pagamento VARCHAR(20) NOT NULL,
                status_pedido VARCHAR(20) NOT NULL,
                id_carrinho UUID NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS eventos_web (
                id_evento SERIAL PRIMARY KEY,
                id_usuario INTEGER REFERENCES dados_clientes(id_usuario),
                id_sessao UUID NOT NULL,
                id_carrinho UUID,
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
