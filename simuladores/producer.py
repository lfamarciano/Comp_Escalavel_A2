import uuid
import random
import json
import time
from faker import Faker
from faker_commerce import Provider
from kafka import KafkaProducer

fake = Faker()
fake.add_provider(Provider)

# Configuração do Produtor Kafka ---
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
def fetch_produtos_from_db():
    pass

def fetch_usuarios_from_db():
    pass


# Funções Geradoras de Dados em Tempo Real
def gerar_transacao_venda(produto, usuario, batch_size=5):
    """Gera um batch de transações de venda para um produto e usuário específicos."""
    transacoes = []
    
    for _ in range(batch_size):
        transacoes.append({
            "id_transacao": str(uuid.uuid4()),
            "id_pedido": str(uuid.uuid4()),
            "id_usuario": usuario["id_usuario"],
            "nome_usuario": usuario["nome_usuario"],
            "id_produto": produto["id_produto"],
            "item": produto["nome_produto"],
            "valor_total_compra": produto["preco_unitario"],
            "data_compra": fake.iso8601(),
            "metodo_pagamento": random.choice(["cartao_credito", "boleto", "pix"]),
            "status_pedido": random.choice(["processando", "enviado", "entregue", "cancelado"]),
            "id_carrinho": str(uuid.uuid4())
        })
            
    yield transacoes

def gerar_evento_web(produto, usuario, batch_size=10):
    """Gera um batch de eventos web para um produto e usuário específicos."""
    tipos_evento = [
        "visualizacao_produto", "adicionar_carrinho", "remover_carrinho",
        "carrinho_criado", "checkout_iniciado", "checkout_concluido"
    ]
    
    eventos = []
    for _ in range(batch_size):
        # 80% dos eventos estão associados a um produto
        id_produto_evento = produto["id_produto"] if random.random() < 0.8 else None

        eventos.append({
            "id_evento": str(uuid.uuid4()),
            "id_usuario": usuario["id_usuario"],
            "id_sessao": str(uuid.uuid4()),
            "id_carrinho": str(uuid.uuid4()),
            "tipo_evento": random.choice(tipos_evento),
            "id_produto": id_produto_evento,
            "timestamp_evento": fake.iso8601()
        })
        
    yield eventos

# Loop Principal para Enviar Dados ao Kafka
def main_producer_loop():
    
    """
    Loop principal que busca dados iniciais e depois entra em um ciclo
    infinito para gerar e enviar dados para o Kafka.
    """
    # Busca os dados uma vez no início da execução
    produtos = fetch_produtos_from_db()
    usuarios = fetch_usuarios_from_db()

    print("Iniciando a geração e envio de dados para o Kafka...")
    while True:
        try:
            # Seleciona um produto e um usuário aleatoriamente para cada evento
            produto_selecionado = random.choice(produtos)
            usuario_selecionado = random.choice(usuarios)

            # Gera um novo batch de transações e envia um por um
            nova_transacao = next(gerar_transacao_venda(produto_selecionado, usuario_selecionado))
            for transacao in nova_transacao:
                producer.send(TRANSACTIONS_TOPIC, value=transacao)
                print(f"Enviado para '{TRANSACTIONS_TOPIC}': {transacao['item']}")

            # Gera um novo batch de eventos web e envia um por um
            novo_evento_web = next(gerar_evento_web(produto_selecionado, usuario_selecionado))
            for evento in novo_evento_web:    
                producer.send(WEB_EVENTS_TOPIC, value=evento)
                print(f"Enviado para '{WEB_EVENTS_TOPIC}': {evento['tipo_evento']}")
            
            # Garante que as mensagens foram enviadas
            producer.flush()

            # Controla a carga do simulador
            time.sleep(random.uniform(1, 3))

        except KeyboardInterrupt:
            print("\nEncerrando o produtor.")
            break
        except Exception as e:
            print(f"Ocorreu um erro: {e}")
            time.sleep(5) # Espera um pouco antes de tentar novamente
            
if __name__ == "__main__":
    main_producer_loop()