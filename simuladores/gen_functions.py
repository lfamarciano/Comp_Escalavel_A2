import uuid
import random
from faker import Faker
from faker_commerce import Provider

fake = Faker()
fake.add_provider(Provider)


# def gerar_catalogo_produtos(n=2):
#     produtos = []
#     for _ in range(n):
#         produtos.append( {
#             "id_produto": str(uuid.uuid4()),
#             "nome_produto": fake.ecommerce_name(),
#             "descricao_produto": fake.sentence(nb_words=6),
#             "categoria": fake.ecommerce_category(),
#             "preco_unitario": round(random.uniform(10, 5000), 2),
#             "estoque_disponivel": random.randint(0, 500)
#         })
#     return produtos

# def gerar_dados_clientes(n=2):
#     segmentos = ["novo", "fiel", "VIP"]
#     clientes = []
#     for _ in range(n):
#         clientes.append( {
#             "id_usuario": str(uuid.uuid4()),
#             "nome_usuario": fake.name(),
#             "email_usuario": fake.safe_email(),
#             "data_cadastro": fake.date_time_between(start_date='-2y', end_date='now').isoformat(),
#             "segmento_cliente": random.choice(segmentos),
#             "cidade": fake.city(),
#             "estado": fake.state(),
#             "pais": fake.country()
#         })
#     return clientes

def gerar_transacoes_vendas(produto, usuario, n=10):
    transacoes = []
    for _ in range(n):
        pedido_id = str(uuid.uuid4())
        carrinho_id = str(uuid.uuid4())
        
        transacoes.append({
            "id_transacao": str(uuid.uuid4()),
            "id_pedido": pedido_id,
            "id_usuario": usuario["id_usuario"],
            "nome_usuario": usuario["nome_usuario"],
            "id_produto": produto["id_produto"],
            "item": produto["nome_produto"],
            "valor_total_compra": produto["preco_unitario"],
            "data_compra": fake.date_time_between(start_date='-6M', end_date='now').isoformat(),
            "metodo_pagamento": random.choice(["cartao_credito", "boleto", "pix"]),
            "status_pedido": random.choice(["processando", "enviado", "entregue", "cancelado"]),
            "id_carrinho": carrinho_id
        })
    return transacoes
        

def gerar_eventos_web(produto, usuario, n=100):
    tipos = [
        "visualizacao_produto", "adicionar_carrinho", "remover_carrinho",
        "carrinho_criado", "checkout_iniciado", "checkout_concluido",
        "pesquisa", "login"
    ]
    eventos = []
    for _ in range(n):
        # só atribui produto em 80% dos eventos
        prod = True if random.random() < 0.8 else None

        eventos.append({
            "id_evento": str(uuid.uuid4()),
            "id_usuario": usuario["id_usuario"],
            "id_sessao": str(uuid.uuid4()),
            "id_carrinho": str(uuid.uuid4()),
            "tipo_evento": random.choice(tipos),
            "id_produto": produto["id_produto"] if prod else None,
            "timestamp_evento": fake.date_time_between(start_date='-6M', end_date='now').isoformat()
        })
        
    return eventos