from kafka import KafkaProducer
import json
from gen_functions import *

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ex.: enviar transações em loop contínuo
for transacao in gerar_transacoes_vendas(
        gerar_dados_clientes(200),
        gerar_catalogo_produtos(50),
        n=1000000
    ):
    producer.send('transacoes_vendas', transacao)
    # time.sleep(0.01)  # ajuste de taxa de geração
