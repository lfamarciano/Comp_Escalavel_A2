import json
import time
import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer
from config import KAFKA_HOST, TRANSACTIONS_TOPIC, WEB_EVENTS_TOPIC, DB_CONFIG

# 1. Configurações
KAFKA_CONSUMER_GROUP = 'db-persistor-group'

# Configurações do Lote
BATCH_SIZE = 100  # Número de mensagens para acumular antes de inserir
BATCH_TIMEOUT = 10 # Segundos para esperar antes de forçar a inserção

# 2. Função de Inserção em Lote
def process_batch(cursor, transacoes_batch, eventos_batch):
    """Insere os lotes de dados no banco de dados usando execute_values para alta performance."""
    if not transacoes_batch and not eventos_batch:
        return

    try:
        if transacoes_batch:
            psycopg2.extras.execute_values(
                cursor,
                """
                INSERT INTO transacoes_vendas (
                    id_pedido, id_usuario, id_produto, quantidade_produto,
                    valor_total_compra, data_compra, metodo_pagamento,
                    status_pedido, id_carrinho
                ) VALUES %s
                """,
                # Prepara a lista de tuplas com os dados na ordem correta
                [(d.get('id_pedido'), d.get('id_usuario'), d.get('id_produto'),
                  d.get('quantidade_produto'), d.get('valor_total_compra'), d.get('data_compra'),
                  d.get('metodo_pagamento'), d.get('status_pedido'), d.get('id_carrinho'))
                 for d in transacoes_batch]
            )
            print(f"  -> Lote de {len(transacoes_batch)} transações inserido.")
        
        if eventos_batch:
            psycopg2.extras.execute_values(
                cursor,
                """
                INSERT INTO eventos_web (
                    id_usuario, id_sessao, id_carrinho,
                    tipo_evento, id_produto, timestamp_evento
                ) VALUES %s
                """,
                [(d.get('id_usuario'), d.get('id_sessao'), d.get('id_carrinho'),
                  d.get('tipo_evento'), d.get('id_produto'), d.get('timestamp_evento'))
                 for d in eventos_batch]
            )
            print(f"  -> Lote de {len(eventos_batch)} eventos web inserido.")
        
        # Faz o commit da transação do lote inteiro
        cursor.connection.commit()

    except psycopg2.Error as e:
        print(f"ERRO ao inserir lote: {e}")
        cursor.connection.rollback()


# 3. Função Principal do Consumidor
def main():
    """Função principal que consome do Kafka e insere no PostgreSQL em lotes."""
    while True:
        try:
            print("Conectando ao Kafka...")
            consumer = KafkaConsumer(
                TRANSACTIONS_TOPIC,
                WEB_EVENTS_TOPIC,
                bootstrap_servers=[KAFKA_HOST],
                group_id=KAFKA_CONSUMER_GROUP,
                auto_offset_reset='earliest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print("Conectado ao Kafka com sucesso!")

            print("\nConectando ao PostgreSQL...")
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            print("Conectado ao PostgreSQL com sucesso!")
            
            transacoes_batch = []
            eventos_batch = []
            last_insert_time = time.time()

            print("\nAguardando mensagens do Kafka para processar em lotes...")
            for message in consumer:
                data = message.value
                
                if message.topic == TRANSACTIONS_TOPIC:
                    transacoes_batch.append(data)
                elif message.topic == WEB_EVENTS_TOPIC:
                    eventos_batch.append(data)
                
                # Verifica se o lote atingiu o tamanho ou o tempo limite
                current_time = time.time()
                if (len(transacoes_batch) + len(eventos_batch) >= BATCH_SIZE or 
                    current_time - last_insert_time >= BATCH_TIMEOUT):
                    
                    process_batch(cursor, transacoes_batch, eventos_batch)
                    
                    # Limpa os lotes e reinicia o contador de tempo
                    transacoes_batch.clear()
                    eventos_batch.clear()
                    last_insert_time = current_time

        except Exception as e:
            print(f"ERRO CRÍTICO no consumidor: {e}")
            print("Tentando reconectar em 10 segundos...")
            time.sleep(10)
        
        finally:
            if 'cursor' in locals() and not cursor.closed:
                cursor.close()
            if 'conn' in locals() and not conn.closed:
                conn.close()
                print("Conexão com o PostgreSQL fechada.")

if __name__ == "__main__":
    main()
