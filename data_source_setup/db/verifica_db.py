import psycopg2

# Configure com os dados do seu banco de dados
DB_CONFIG = {
    'host': 'localhost',
    'user': 'postgres',
    'password': '123',  # Lembre-se de usar a sua senha real aqui
    'port': '5432',
    'database': 'ecommerce_db'
}

def check_row_counts():
    """Conecta ao banco de dados e imprime a contagem de linhas de cada tabela."""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
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
