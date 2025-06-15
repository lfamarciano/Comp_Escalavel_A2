import uuid
import random
import json
import time
from faker import Faker
from faker_commerce import Provider
from kafka import KafkaProducer

fake = Faker()
fake.add_provider(Provider)

# Configuração do Produtor Kafka 
KAFKA_BROKER_URL = 'localhost:9092' # URL do broker Kafka
TRANSACTIONS_TOPIC = 'transacoes_vendas'
WEB_EVENTS_TOPIC = 'eventos_web'

# Serializador JSON para enviar os dicionários como bytes
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER_URL],
    value_serializer=json_serializer
)

# Funções para Buscar Dados Iniciais do Banco de Dados
# mockado por enquanto
categoria_produto = ["eletronicos", "moda", "casa", "esporte", "beleza", "livros"]
def fetch_produtos_from_db():
    
    return [{"id_produto":str(uuid.uuid4()), "nome_produto": fake.word(), "categoria_produto":random.choice(categoria_produto), "preco_unitario":random.randint(0,1000)} for _ in range(100)]

def fetch_usuarios_from_db():
    return [{"id_usuario":str(uuid.uuid4()), "nome_usuario": fake.name()} for _ in range(500)]

    
def simular_atividade_cliente(usuario, todos_produtos, bprint=False):
    """
    Simula uma jornada completa do cliente, desde o login,
    e envia os eventos correspondentes para o Kafka.
    """
    id_sessao = str(uuid.uuid4())
    print(f"[INÍCIO DA JORNADA] Usuário {usuario['nome_usuario']} ---") if bprint else None

    # Login
    evento_login = {
        "id_evento": str(uuid.uuid4()),
        "id_usuario": usuario["id_usuario"],
        "id_sessao": id_sessao,
        "tipo_evento": "login",
        "id_carrinho": None,
        "id_produto": None,
        "timestamp_evento": fake.iso8601()
    }
    producer.send(WEB_EVENTS_TOPIC, value=evento_login)
    print(f"-> Evento enviado: {evento_login['tipo_evento']}") if bprint else None

    # visualização de produtos
    for _ in range(random.randint(1, 4)):
        produto = random.choice(todos_produtos)
        evento_visualizacao = {
            "id_evento": str(uuid.uuid4()),
            "id_usuario": usuario["id_usuario"],
            "id_sessao": id_sessao,
            "tipo_evento": "visualizacao_produto",
            "id_carrinho": None,
            "id_produto": produto["id_produto"],
            "timestamp_evento": fake.iso8601()
        }
        producer.send(WEB_EVENTS_TOPIC, value=evento_visualizacao)
        print(f"-> Evento enviado: {evento_visualizacao['tipo_evento']} (Produto: {produto['nome_produto']})") if bprint else None
        
    # 15% de chance de ficar inativo
    if random.random() < 0.15:
        print("[FIM DA JORNADA] Usuário ficou INATIVO após o login.") if bprint else None
        return
    
    # Criação do Carrinho
    id_carrinho = str(uuid.uuid4())
    evento_carrinho_criado = {
        "id_evento": str(uuid.uuid4()), "id_usuario": usuario["id_usuario"], "id_sessao": id_sessao,
        "tipo_evento": "carrinho_criado", "id_carrinho": id_carrinho, "id_produto": None,
        "timestamp_evento": fake.iso8601()
    }
    producer.send(WEB_EVENTS_TOPIC, value=evento_carrinho_criado)
    print(f"-> Evento enviado: {evento_carrinho_criado['tipo_evento']}") if bprint else None
    # time.sleep(random.uniform(0.1, 0.5)) # espera um pouco ante de adicionar 

    # adiciona itens ao carrinho
    num_produtos = random.randint(1, 4)
    produtos = random.sample(todos_produtos, num_produtos)
    for prod in produtos: 
        evento_add_carrinho = {
            "id_evento": str(uuid.uuid4()), "id_usuario": usuario["id_usuario"], "id_sessao": id_sessao,
            "tipo_evento": "add_prod_carrinho", "id_carrinho": id_carrinho, "id_produto": prod["id_produto"],
            "timestamp_evento": fake.iso8601()
        }
        producer.send(WEB_EVENTS_TOPIC, value=evento_add_carrinho)
        print(f"-> Evento enviado: {evento_add_carrinho['tipo_evento']} (Produto: {prod['nome_produto']})") if bprint else None
        time.sleep(random.uniform(0.1, 0.5))

    # Decisão de Conversão (30% de chance de converter o carrinho)
    if random.random() < 0.30:
        evento_checkout = {
            "id_evento": str(uuid.uuid4()), "id_usuario": usuario["id_usuario"], "id_sessao": id_sessao,
            "tipo_evento": "checkout_concluido", "id_carrinho": id_carrinho, "id_produto": None,
            "timestamp_evento": fake.iso8601()
        }
        producer.send(WEB_EVENTS_TOPIC, value=evento_checkout)
        print(f"-> Evento enviado: {evento_checkout['tipo_evento']}") if bprint else None

        # Gera as transações
        for prod in produtos:
            transacao = {
                "id_transacao": str(uuid.uuid4()), "id_pedido": str(uuid.uuid4()), "id_usuario": usuario["id_usuario"],
                "nome_usuario": usuario["nome_usuario"], "id_produto": prod["id_produto"], "categoria_produto": prod["categoria_produto"],
                "item": prod["nome_produto"], "valor_total_compra": prod["preco_unitario"],
                "data_compra": evento_checkout["timestamp_evento"],
                "metodo_pagamento": random.choice(["cartao_credito", "boleto", "pix"]),
                "status_pedido": "processando", "id_carrinho": id_carrinho
            }
            producer.send(TRANSACTIONS_TOPIC, value=transacao)
            print(f"-> Transação enviada para o item '{transacao['item']}'") if bprint else None
    else:
        print("[FIM DA JORNADA] Usuário ABANDONOU o carrinho.") if bprint else None

# Loop principal para enviar dados ao Kafka
def main_producer_loop(time_sleep=5):
    produtos = fetch_produtos_from_db()
    usuarios = fetch_usuarios_from_db()

    if not produtos or not usuarios:
        print("ERRO: Não foi possível carregar dados de produtos ou usuários.")
        return

    print("\nIniciando a simulação de atividade de clientes...")
    while True:
        try:
            usuario_selecionado = random.choice(usuarios)
            
            # A função agora recebe a lista completa de produtos
            simular_atividade_cliente(usuario_selecionado, produtos)
            
            producer.flush() 
            
            time.sleep(random.uniform(1, 3))

        except KeyboardInterrupt:
            print("\nEncerrando o produtor.")
            break
        except Exception as e:
            print(f"Ocorreu um erro: {e}")
            time.sleep(time_sleep)
            
if __name__ == "__main__":
    main_producer_loop()