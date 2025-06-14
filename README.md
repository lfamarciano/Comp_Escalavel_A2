# Trabalho A2 de Computação Escalável 
## Fontes de dados
- transacoes_vendas -> tv
	- id_transacao: UUID (identificador único da transação)
	- id_pedido: UUID (identificador único do pedido)
	- id_usuario: UUID
	- id_produto: UUID (lista ou array de IDs se houver múltiplos produtos no pedido)
	- quantidade_produto: Inteiro (lista ou array de quantidades)
	- valor_total_compra: Float
	- data_compra: Timestamp (ISO 8601)
	- metodo_pagamento: String (e.g., "cartao_credito", "boleto", "pix")
	- status_pedido: String (e.g., "processando", "enviado", "entregue", "cancelado")
	- id_carrinho: UUID (para correlação com eventos de carrinho)
- eventos web -> ew
	- id_evento: UUID
	- id_usuario: UUID (se logado)
	- id_sessao: UUID
	- id_carrinho: UUID (se o evento está relacionado ao carrinho)
	- tipo_evento: String (e.g., "visualizacao_produto", "adicionar_carrinho", "remover_carrinho", "carrinho_criado", "checkout_iniciado", "checkout_concluido", "pesquisa", "login")
	- id_produto: UUID (se o evento estiver relacionado a um produto)
	- timestamp_evento: Timestamp (ISO 8601)

- catalogo_produtos -> cp
	- id_produto: UUID
	- nome_produto: String
	- descricao_produto: String
	- categoria: String (e.g., "eletronicos", "roupas", "livros")
	- preco_unitario: Float
	- estoque_disponivel: Inteiro (se quiser simular alterações de estoque)

- dados_clientes -> dc
	- id_usuario: UUID
	- nome_usuario: String
	- email_usuario: String
	- data_cadastro: Timestamp
	- segmento_cliente: String (e.g., "novo", "fiel", "VIP")
	- cidade, estado, pais: String (para análise geográfica)

## Métricas
- vendas totais/período de tempo
  - tv
- numero de pedidos/período de tempo
  - tv
- ticket médio
  - tv
- produtos mais vendidos
  - tv
- receita por categoria de produto
  - tv e ew
- taxa de conversão de carrinho
  - tv e ew
- receita média por usuário
  - tv e dc
- Crescimento de receita (histórico)
  - tv
- Variação do ticket médio (histórico)
  - tv
- Produtos mais vendidos ao longo do tempo (histórico)
  - tv
- Taxa de abandono de carrinho(histórico)
  - tv e ew
- Média de pedidos por cliente
  - tv e dc

# Modelagem
## Simuladores
Processos (ou máquinas) gerando dados com Faker e enviando para o Pub Sub

## Etapas da modelagem
### Broker
#### Dados em tempo real
Recebe os dados dos simuladores e envia para:
- Banco de dados relacional para persistir o dado a longo prazo
- Spark streaming para processamento de dados em tempo real

#### Dados históricos
Recebe os dados da base de dados relacional e envia para o processamento em batch no Spark

### Pipeline de dados
#### Dados em tempo real
Utilização do Spark Streaming, que tem tecnologia e estratégias específicas para o processamento de dados com baixa latência

#### Dados históricos
Processamento de todos o histórico de dados salvo no banco de dados relacional de uma só vez para termos dados históricos agregados


## Armazenamto de baixa latência (somente para dados em tempo real)
Escolha do Redis como opção de um armazenamento pequeno, porém rápido, de onde o dashboard lerá os dados em tempo real mais recentes em intervalos curtos (0,5 - 1 segundo).

Dados "antigos", que não fazem sentido para um contexto de tempo real serão deletados para evitar sobrecorregar a memória.

## Dashboard
Exibição das estatísticas tanto históricas quanto em tempo real com Streamlit.

## Escolhas de projeto
- Simuladores paralelos utilizando Faker para geração de dados
- Base de dados relacional (PostgreSQL ou SQLite) para dados históricos e persistência de dados novos para atualização posterior de métricas históricas
- Spark para processamento eficiente em um único Batch de todos os dados históricos
- Spark streaming para processamento eficiente de dados contínuos para que métricas em tempo real
- Redis para persistir a versão mais recente das métricas em tempo real em uma estrutura de baixa latência, para que o Dashboard possa se atualizar continuanemte

## Dados históricos
As métricas de dados históricos serão calculadas a primeira vez lendo todos os dados (simulados) já armazenados na base de dados no começo de uma execução de todo o pipeline.

Com a geração de novos dados pelos simuladores, esses serão salvos para acesso posterior na base de dados relacional.

Com isso, é possível simular a mudança do estado dos dados históricos mês-a-mês ou dia-a-dia por exemplo, com o cálculo de todos os dados históricos (iniciais e adicionados depois pelos simuladores) sendo recalculados pelo Spark uma vez por dia.

## Dados em tempo real
Conforme os simuladores geram os dados o Spark Streaming vai fazer um processamento eficiente que permite que um dado gerado no começo chegue o mais rápido possível no dashboard sem que hajam problemas como dados chegando fora de ordem, por exemplo.

Além disso, utilizando o Redis como estrutura de armazenamento em memória, temos uma confiabilidade e disponibilidade dos dados sem comprometer a baixa latência.
