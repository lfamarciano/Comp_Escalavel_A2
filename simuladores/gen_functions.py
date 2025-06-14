import uuid
import random
import json
from faker import Faker
from faker.providers import commerce

fake = Faker()
fake.add_provider(commerce)

def gerar_catalogo_produtos(n=50):

    for _ in range(n):
        yield {
            "id_produto": str(uuid.uuid4()),
            "nome_produto": fake.ecommerce_name(),
            "descricao_produto": fake.sentence(nb_words=6),
            "categoria": fake.ecommerce_department(),
            "preco_unitario": round(random.uniform(10, 5000), 2),
            "estoque_disponivel": random.randint(0, 500)
        }

def gerar_dados_clientes(n=200):

    segmentos = ["novo", "fiel", "VIP"]
    for _ in range(n):
        yield {
            "id_usuario": str(uuid.uuid4()),
            "nome_usuario": fake.name(),
            "email_usuario": fake.safe_email(),
            "data_cadastro": fake.date_time_between(start_date='-2y', end_date='now').isoformat(),
            "segmento_cliente": random.choice(segmentos),
            "cidade": fake.city(),
            "estado": fake.state(),
            "pais": fake.country()
        }

def gerar_transacoes_vendas(clientes, produtos, n=1000):

    clientes = list(clientes)
    produtos = list(produtos)
    for _ in range(n):
        user_id = random.choice(clientes)["id_usuario"]
        pedido_id = str(uuid.uuid4())
        carrinho_id = str(uuid.uuid4())
        itens = []
        for pid in random.sample(produtos, k=random.randint(1,4)):
            qtd = random.randint(1,5)
            itens.append({
                "id_produto": pid["id_produto"],
                "quantidade": qtd,
                "preco_unitario": pid["preco_unitario"]
            })
        total = round(sum(i["quantidade"] * i["preco_unitario"] for i in itens), 2)
        yield {
            "id_transacao": str(uuid.uuid4()),
            "id_pedido": pedido_id,
            "id_usuario": user_id,
            "itens": itens,
            "valor_total_compra": total,
            "data_compra": fake.date_time_between(start_date='-6M', end_date='now').isoformat(),
            "metodo_pagamento": random.choice(["cartao_credito", "boleto", "pix"]),
            "status_pedido": random.choice(["processando", "enviado", "entregue", "cancelado"]),
            "id_carrinho": carrinho_id
        }

def gerar_eventos_web(clientes, produtos, n=3000):
    tipos = [
        "visualizacao_produto", "adicionar_carrinho", "remover_carrinho",
        "carrinho_criado", "checkout_iniciado", "checkout_concluido",
        "pesquisa", "login"
    ]
    clientes = list(clientes)
    produtos = list(produtos)
    for _ in range(n):
        usr = random.choice(clientes)["id_usuario"]
        sessao = str(uuid.uuid4())
        carrinho = str(uuid.uuid4())
        tipo = random.choice(tipos)
        # só atribui produto em 80% dos eventos
        prod = random.choice(produtos)["id_produto"] if random.random() < 0.8 else None

        yield {
            "id_evento": str(uuid.uuid4()),
            "id_usuario": usr,
            "id_sessao": sessao,
            "id_carrinho": carrinho,
            "tipo_evento": tipo,
            "id_produto": prod,
            "timestamp_evento": fake.date_time_between(start_date='-6M', end_date='now').isoformat()
        }
