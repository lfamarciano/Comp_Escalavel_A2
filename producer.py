# ANTES DE EXECUTAR ESTE ARQUIVO, CERTIFIQUE-SE DE QUE O DOCKER DESCKTOP ESTÁ INSTALADO
# RODE O SEGUINTE COMANDO NO TERMINAL PARA CRIAR E INICIAR OS CONTEINERES:
# > docker-compose up
# PARA VERIFICAR SE OS CONTEINERES ESTÃO RODANDO, USE O COMANDO:
# > docker ps
# JÁ PODE RODAR ESSE ARQUIVO (producers.py) PARA COMEÇAR A SIMULAR A ATIVIDADE DOS USUÁRIOS
# ACESSE http://localhost:9000 PARA VER A INTEFACE DO Kafdrop
# PARA PARAR OS CONTEINERES, USE O COMANDO:
# > docker-compose down

import uuid
import random
import json
from decimal import Decimal
import time
from datetime import datetime, timedelta
import os
import multiprocessing
from faker import Faker
# from faker_commerce import Provider
from kafka import KafkaProducer
import psycopg2
import psycopg2.extras
from db.db_config import DB_CONFIG

fake = Faker()
# fake.add_provider(Provider)

# Configuração do Produtor Kafka 
KAFKA_BROKER_URL = 'localhost:9092'
TRANSACTIONS_TOPIC = 'transacoes_vendas'
WEB_EVENTS_TOPIC = 'eventos_web'

# Classe para ensinar a biblioteca JSON a converter o tipo Decimal e datetime
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        # Se o objeto for do tipo Decimal, converte para float
        if isinstance(obj, Decimal):
            return float(obj)
        # Se o objeto for do tipo datetime, converte para string no formato ISO
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(CustomJSONEncoder, self).default(obj)



# Funções para buscar dados do PostgreSQL
def fetch_produtos_from_db():
    """
    Busca os dados de produtos diretamente do banco de dados PostgreSQL
    e retorna uma lista de dicionários.
    """
    print("Buscando produtos do banco de dados real...")
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Seleciona as colunas necessárias para a simulação
        cursor.execute("SELECT id_produto, nome_produto, categoria, preco_unitario FROM catalogo_produtos;")
        
        produtos = cursor.fetchall()
        
        # Converte os resultados do cursor que são como dicionários para dicionários padrão
        produtos_lista = [dict(row) for row in produtos]
        
        print(f"-> {len(produtos_lista)} produtos encontrados no banco de dados.")
        return produtos_lista
        
    except psycopg2.Error as e:
        print(f"Erro ao buscar produtos do banco de dados: {e}")
        return [] # Retorna uma lista vazia em caso de erro
    finally:
        if conn:
            conn.close()

def fetch_usuarios_from_db():
    """
    Busca os dados de usuários diretamente do banco de dados PostgreSQL
    e retorna uma lista de dicionários.
    """
    print("Buscando usuários do banco de dados real...")
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Seleciona as colunas necessárias para a simulação
        cursor.execute("SELECT id_usuario, nome_usuario FROM dados_clientes;")
        
        usuarios = cursor.fetchall()
        
        usuarios_lista = [dict(row) for row in usuarios]

        print(f"-> {len(usuarios_lista)} usuários encontrados no banco de dados.")
        return usuarios_lista
        
    except psycopg2.Error as e:
        print(f"Erro ao buscar usuários do banco de dados: {e}")
        return [] # Retorna uma lista vazia em caso de erro
    finally:
        if conn:
            conn.close()

    
def simular_atividade_cliente(producer, usuario, todos_produtos, bprint=True):
    """
    Simula uma jornada completa do cliente, desde o login,
    e envia os eventos correspondentes para o Kafka.
    """
    id_sessao = str(uuid.uuid4())
    print(f"[INÍCIO DA JORNADA] Usuário {usuario['nome_usuario']} ---") if bprint else None

    # Usamos o tempo atual como base para toda a jornada
    tempo_base_jornada = datetime.now()
    
    # Login
    evento_login = {
        "id_evento": str(uuid.uuid4()),
        "id_usuario": usuario["id_usuario"],
        "id_sessao": id_sessao,
        "tipo_evento": "login",
        "id_carrinho": None,
        "id_produto": None,
        "timestamp_evento": tempo_base_jornada - timedelta(seconds=random.randint(30, 60))
    }
    producer.send(WEB_EVENTS_TOPIC, value=evento_login)
    print(f"-> Evento enviado: {evento_login['tipo_evento']}") if bprint else None

    # visualização de produtos
    for i,_ in enumerate(range(random.randint(1, 4))):
        produto = random.choice(todos_produtos)
        evento_visualizacao = {
            "id_evento": str(uuid.uuid4()),
            "id_usuario": usuario["id_usuario"],
            "id_sessao": id_sessao,
            "tipo_evento": "visualizacao_produto",
            "id_carrinho": None,
            "id_produto": produto["id_produto"],
            "timestamp_evento": tempo_base_jornada - timedelta(seconds=random.randint(5, 14 - i))
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
        "timestamp_evento": tempo_base_jornada - timedelta(seconds=random.randint(15, 29))
    }
    producer.send(WEB_EVENTS_TOPIC, value=evento_carrinho_criado)
    print(f"-> Evento enviado: {evento_carrinho_criado['tipo_evento']}") if bprint else None
    # time.sleep(random.uniform(0.1, 0.5)) # espera um pouco ante de adicionar 

    # adiciona itens ao carrinho
    produtos_no_carrinho = random.sample(todos_produtos, random.randint(1, 4))
    for i, prod in enumerate(produtos_no_carrinho):
        evento_add_carrinho = {
            "id_evento": str(uuid.uuid4()), "id_usuario": usuario["id_usuario"], "id_sessao": id_sessao,
            "tipo_evento": "produto_adicionado_carrinho", "id_carrinho": id_carrinho, "id_produto": prod["id_produto"],
            "timestamp_evento": tempo_base_jornada - timedelta(seconds=random.randint(5, 14 - i))
        }
        producer.send(WEB_EVENTS_TOPIC, value=evento_add_carrinho)
        print(f"-> Evento enviado: {evento_add_carrinho['tipo_evento']} (Produto: {prod['nome_produto']})") if bprint else None
        time.sleep(random.uniform(0.1, 0.5))

    # Decisão de Conversão (30% de chance de converter o carrinho)
    if random.random() < 0.30:
        if bprint: print("-> Usuário decidiu CONVERTER.")
        evento_checkout = {
            "id_evento": str(uuid.uuid4()), "id_usuario": usuario["id_usuario"], "id_sessao": id_sessao,
            "tipo_evento": "checkout_concluido", "id_carrinho": id_carrinho, "id_produto": None,
            "timestamp_evento": tempo_base_jornada # O checkout acontece no tempo base
        }
        producer.send(WEB_EVENTS_TOPIC, value=evento_checkout)

        # Gera as transações
        for prod in produtos_no_carrinho:
            transacao = {
                "id_transacao": str(uuid.uuid4()), "id_pedido": str(uuid.uuid4()), "id_usuario": usuario["id_usuario"],
                "nome_usuario": usuario["nome_usuario"], "id_produto": prod["id_produto"], "categoria": prod["categoria"],
                "item": prod["nome_produto"], "valor_total_compra": prod["preco_unitario"], "quantidade_produto":random.randint(1, 3),
                "data_compra": evento_checkout["timestamp_evento"],
                "metodo_pagamento": random.choice(["cartao_credito", "boleto", "pix"]),
                "status_pedido": "processando", "id_carrinho": id_carrinho
            }
            producer.send(TRANSACTIONS_TOPIC, value=transacao)
            print(f"-> Transação enviada para o item '{transacao['item']}'") if bprint else None
    else:
        print("[FIM DA JORNADA] Usuário ABANDONOU o carrinho.") if bprint else None
        
# Função do Worker
def worker_producer(worker_id, usuarios, produtos):
    """
    Esta é a função que cada processo irá executar de forma independente.
    """
    print(f"[Worker-{worker_id} PID:{os.getpid()}] Iniciando...")

    # CADA PROCESSO CRIA SUA PRÓPRIA INSTÂNCIA DO PRODUCER
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER_URL],
        value_serializer=lambda v: json.dumps(v, cls=CustomJSONEncoder).encode('utf-8'),
        # Configurações de performance do Kafka Producer
        # Aumenta o tamanho do lote de mensagens para 64KB.
        batch_size=16384 * 4, 
        
        # Espera até 5ms para preencher o lote, mesmo que não esteja cheio. Reduz requisições de rede.
        linger_ms=5, 
        
        # Aumenta o buffer total de memória para 64MB para absorver picos de produção.
        buffer_memory=67108864 
    )
    
    while True:
        try:
            usuario_selecionado = random.choice(usuarios)
            simular_atividade_cliente(producer, usuario_selecionado, produtos,bprint=True)
            producer.flush()
            time.sleep(random.uniform(0.5, 2.0))
        except Exception as e:
            print(f"[Worker-{worker_id} PID:{os.getpid()}] Erro: {e}")
            time.sleep(5)
            
# Orquestrador Principal
if __name__ == "__main__":    
    # Define quantos produtores paralelos você quer rodar
    NUM_PROCESSES = 4

    # Busca os dados do DB uma única vez no processo principal
    usuarios_db = fetch_usuarios_from_db()
    produtos_db = fetch_produtos_from_db()

    if not usuarios_db or not produtos_db:
        print("ERRO FATAL: Não foi possível carregar dados do banco de dados. Abortando.")
    else:
        processes = []
        print(f"\nIniciando {NUM_PROCESSES} processos produtores...")

        # Cria e inicia cada worker
        for i in range(NUM_PROCESSES):
            process = multiprocessing.Process(
                target=worker_producer,
                args=(i + 1, usuarios_db, produtos_db)
            )
            processes.append(process)
            process.start()
        
        # Espera que os processos terminem
        for process in processes:
            process.join()
