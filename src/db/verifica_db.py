import psycopg2
import sys
from pathlib import Path

# Adiciona o diretório pai ao PYTHONPATH
sys.path.append(str(Path(__file__).parent.parent))
from config import DB_CONFIG, OUT_URL

HOST = 
conn_config_admin = {**DB_CONFIG, 'host': OUT_URL}


def check_row_counts():
    """Conecta ao banco de dados e imprime a contagem de linhas de cada tabela."""
    conn = None
    try:
        conn = psycopg2.connect(**conn_config_admin)
        with conn.cursor() as cursor:
            
            tabelas = [
                'dados_clientes',
                'catalogo_produtos',
                'eventos_web',
                'transacoes_vendas'
            ]
            
            print("--- Contagem de Linhas no Banco de Dados ---")
            
            for tabela in tabelas:
                # Executa a query para contar as linhas
                cursor.execute(f"SELECT COUNT(*) FROM {tabela};")
                
                # Pega o primeiro (e único) resultado
                resultado = cursor.fetchone()
                contagem = resultado[0] if resultado else 0
                
                print(f"- Tabela '{tabela}': {contagem} linhas")

    except psycopg2.Error as e:
        print(f"Erro de banco de dados: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    check_row_counts()
