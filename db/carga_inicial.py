import psycopg2
import psycopg2.extras
from faker import Faker
from faker_commerce import Provider
import random
from datetime import timedelta
from create_db import DB_CONFIG
import uuid

fake = Faker('pt_BR')
fake.add_provider(Provider)

# Funções de Geração de Dados

def limpar_dados(cursor):
    """Limpa os dados de todas as tabelas para garantir uma carga nova."""
    print("Limpando dados antigos das tabelas...")
    # A ordem é importante devido às chaves estrangeiras
    cursor.execute("TRUNCATE TABLE eventos_web, transacoes_vendas, catalogo_produtos, dados_clientes RESTART IDENTITY CASCADE;")
    print("Tabelas limpas com sucesso.")

def inserir_clientes(cursor, num_clientes=200):
    """Gera e insere clientes na tabela dados_clientes."""
    print(f"Gerando {num_clientes} clientes...")
    clientes = []
    for _ in range(num_clientes):
        clientes.append((
            fake.name(),
            fake.unique.email(),
            fake.date_between(start_date='-2y', end_date='-1y'), # Clientes se cadastraram há 1-2 anos
            random.choice(['novo', 'fiel', 'VIP']),
            fake.city(),
            fake.state_abbr()
        ))
    
    psycopg2.extras.execute_values(
        cursor,
        "INSERT INTO dados_clientes (nome_usuario, email_usuario, data_cadastro, segmento_cliente, cidade, estado) VALUES %s",
        clientes
    )

def inserir_produtos(cursor, num_produtos=100):
    """Gera e insere produtos no catálogo."""
    print(f"Gerando {num_produtos} produtos...")
    produtos = []
    categorias = ['Eletrônicos', 'Roupas', 'Livros', 'Casa e Cozinha', 'Esportes', 'Brinquedos']
    for _ in range(num_produtos):
        produtos.append((
            fake.ecommerce_name(),
            fake.text(max_nb_chars=200),
            random.choice(categorias),
            round(random.uniform(10.5, 999.9), 2),
            random.randint(10, 200)
        ))
    
    psycopg2.extras.execute_values(
        cursor,
        "INSERT INTO catalogo_produtos (nome_produto, descricao_produto, categoria, preco_unitario, estoque_disponivel) VALUES %s",
        produtos
    )

def simular_historico_jornadas(cursor, clientes, produtos, num_jornadas=200):
    """Simula jornadas de clientes completas, gerando eventos e transações."""
    print(f"Simulando {num_jornadas} jornadas de compra...")
    eventos_web = []
    transacoes_vendas = []

    for _ in range(num_jornadas):
        # Seleciona um cliente e a data de sua jornada
        cliente = random.choice(clientes)
        id_usuario = cliente[0]
        data_cadastro_cliente = cliente[1]
        timestamp_jornada = fake.date_time_between(start_date=data_cadastro_cliente, end_date='now')

        # Simula a jornada
        id_sessao = uuid.uuid4()
        id_carrinho = uuid.uuid4()

        # Evento de Login
        eventos_web.append((id_usuario, id_sessao, None, 'login', None, timestamp_jornada - timedelta(minutes=10)))

        # Visualização de produtos
        for _ in range(random.randint(1, 5)):
             produto_visto = random.choice(produtos)
             eventos_web.append((id_usuario, id_sessao, None, 'visualizacao_produto', produto_visto[0], timestamp_jornada - timedelta(minutes=random.randint(5, 9))))

        # 85% de não abandonar a sessão
        if random.random() < 0.85:
            eventos_web.append((id_usuario, id_sessao, id_carrinho, 'carrinho_criado', None, timestamp_jornada - timedelta(minutes=5)))
            
            # Adiciona produtos ao carrinho
            produtos_no_carrinho = random.sample(produtos, k=random.randint(1, 3))
            for produto_no_carrinho in produtos_no_carrinho:
                eventos_web.append((id_usuario, id_sessao, id_carrinho, 'produto_adicionado_carrinho', produto_no_carrinho[0], timestamp_jornada - timedelta(minutes=random.randint(2, 4))))

            # 30% de chance de comprar
            if random.random() < 0.30:
                eventos_web.append((id_usuario, id_sessao, id_carrinho, 'checkout_concluido', None, timestamp_jornada))
                
                # Gera as transações para cada item no carrinho
                id_pedido = uuid.uuid4()
                for produto_comprado in produtos_no_carrinho:
                    id_produto = produto_comprado[0]
                    preco_unitario = produto_comprado[1]
                    quantidade = 1 # Simplificando para quantidade 1
                    
                    transacoes_vendas.append((
                        id_pedido, id_usuario, id_produto, quantidade, preco_unitario * quantidade,
                        timestamp_jornada, random.choice(["cartao_credito", "boleto", "pix"]),
                        random.choice(["entregue", "enviado"]), id_carrinho
                    ))

    # Inserir todos os eventos e transações gerados no banco de dados
    psycopg2.extras.execute_values(
        cursor,
        "INSERT INTO eventos_web (id_usuario, id_sessao, id_carrinho, tipo_evento, id_produto, timestamp_evento) VALUES %s",
        eventos_web
    )


    if transacoes_vendas:
        psycopg2.extras.execute_values(
            cursor,
            "INSERT INTO transacoes_vendas (id_pedido, id_usuario, id_produto, quantidade_produto, valor_total_compra, data_compra, metodo_pagamento, status_pedido, id_carrinho) VALUES %s",
            transacoes_vendas
        )

    
    print(f"Total de {len(eventos_web)} eventos web e {len(transacoes_vendas)} transações gerados.")



def main():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        psycopg2.extras.register_uuid()
        
        with conn.cursor() as cursor:
            limpar_dados(cursor)
            
            inserir_clientes(cursor)
            inserir_produtos(cursor)
            
            cursor.execute("SELECT id_usuario, data_cadastro FROM dados_clientes")
            clientes_db = cursor.fetchall()
            
            cursor.execute("SELECT id_produto, preco_unitario FROM catalogo_produtos")
            produtos_db = cursor.fetchall()
            
            if clientes_db and produtos_db:
                simular_historico_jornadas(cursor, clientes_db, produtos_db)
            else:
                print("Não há clientes ou produtos no DB para gerar o histórico de jornadas.")

            conn.commit()
            print("\nCarga inicial de dados concluída com sucesso!")
            
    except psycopg2.Error as e:
        print(f"Erro de banco de dados: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    main()
