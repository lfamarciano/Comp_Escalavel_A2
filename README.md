# Configuração do ambiente de desenvolvimento

## Arquivo `.env`

Crie um arquivo `.env` na raíz do projeto com o seguinte conteúdo:

```config
DB_CONFIG={"host": "localhost", "user": "postgres", "password": {senha do seu psql}, "port": {porta do seu psql}(a padrao é 5432), "database": "ecommerce_db"}
```

Isso vai garantir que você não precise alterar para suas configurações do PostgreSQL (e outras ferramentas) em cada script que forem necessárias.

## PostgreSQL

Baixe e execute o instalador de Windows para a versão mais recente do PostgreSQL (17.5) no site [Enterprise DB](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads)

## Docker Desktop

Baixe e execute o instalador de Windows para a versão mais recente do Docker Desktop na [página oficial do Docer](https://www.docker.com/products/docker-desktop/)

## Baixando dependências python
Nesse projeto estamos utilizando `uv` como gerenciador de dependências. O `uv` é um gerenciador de dependências altamente eficiente do python que resolve quaisquer problemas de dependência automaticamente enquanto garante uma instalação muito mais rápida do que simplesmente utilizando o `pip`

1. Garanta que o pacote `uv` está instalado no seu python
   - Rode `pip install uv`
2. A partir daqui, o `uv` irá gerenciar nossas dependências
3. Crie um *ambiente virtual* com `uv venv`
4. Ative o ambiente com `.\.venv\Scripts\activate.bat` (ou activate.ps1 no PowerShell)
5. Instale as dependências no ambiente com `uv sync`
6. Sempre que for adicionar uma dependência, use `uv add {pacote_python}`, no lugar de `pip install {pacote_python}` 
   - Exemplo: `uv add pandas` ao invés de `pip install pandas`


## GUIA PRA CONFIGURAR O SPARK NO WINDOWS
### Passo 1: Instalar o Java Development Kit (JDK) correto
O Spark é uma aplicação Java e precisa do JDK para rodar. A versão do Spark que estamos usando (4.0.0) exige o Java 17.

1. Baixe o JDK 17: 
   - Recomendamos o OpenJDK da Adoptium, que é gratuito e confiável
   - Link de Download: (Adoptium OpenJDK 17 LTS)[https://adoptium.net/temurin/releases/?version=17]
2. Selecione Windows e x64, e baixe o instalador `.msi`.
3. Instale o Java: Execute o arquivo `.msi`.
   - PONTO CRÍTICO: Durante a instalação, em "Custom Setup", certifique-se de que a opção "Set JAVA_HOME variable" esteja marcada para ser instalada. Isso configura a variável de ambiente `JAVA_HOME` automaticamente.

### Passo 2: Baixar e Descompactar o Spark
Agora, vamos baixar o "motor" do Spark.

1. Acesse a página de downloads do Spark: (Apache Spark Downloads) [https://spark.apache.org/downloads.html]

2. Selecione as versões corretas:
   - `Choose a Spark release`: 4.0.0
   - `Choose a package type`: Pre-built for Apache Hadoop 3.4 and later
3. Baixe o arquivo `.tgz` clicando no primeiro link de download.
4. Crie e Descompacte:
- Crie uma pasta simples na raiz do seu disco, por exemplo: `C:\spark`.
- Use o 7-Zip para descompactar o arquivo .tgz que você baixou. Você precisará fazer isso em duas etapas:
- Extraia o arquivo `spark-4.0.0-bin-hadoop3.tgz` para obter um arquivo `spark-4.0.0-bin-hadoop3.tar`.
- Extraia o arquivo .tar para dentro da pasta `C:\spark`.
- Ao final, a estrutura de pastas deve ser: `C:\spark\spark-4.0.0-bin-hadoop3`.

### Passo 3: Configurar a Camada de Compatibilidade do Hadoop
Esta é a etapa mais importante para evitar o erro UnsatisfiedLinkError. Precisamos de dois arquivos do Hadoop.

1. Crie a Estrutura de Pastas:
   - Crie uma pasta hadoop na raiz do seu disco: `C:\hadoop`.
   - Dentro dela, crie uma pasta bin: `C:\hadoop\bin`.
2. Baixe os Arquivos Necessários:
   - O Spark 4.0 foi compilado com o Hadoop 3.4, mas usaremos os utilitários da versão 3.3.6, que são compatíveis.
   - Baixe o `winutils.exe` e o `hadoop.dll` do seguinte link:
   - https://github.com/cdarlint/winutils/blob/master/hadoop-3.3.6/bin/winutils.exe
   - Baixe a pasta referente a versão correta
3. Coloque os arquivos na pasta bin:
   - Certifique-se de que ambos os arquivos, `winutils.exe` e `hadoop.dll`, estejam dentro de `C:\hadoop\bin`.
4. Crie um diretório `C:/tmp/spark_checkpoints`.

### Passo 4: Configurar as Variáveis de Ambiente
Aqui, vamos dizer ao Windows onde encontrar o Java, o Spark e o Hadoop.

1. Abra as Variáveis de Ambiente:
   - No menu Iniciar, pesquise por Editar as variáveis de ambiente do sistema e abra.
   - Clique no botão Variáveis de Ambiente....
2. Crie as Variáveis de Sistema (na seção de baixo):
   - Clique em "Novo..." para cada uma das seguintes variáveis:
   - JAVA_HOME:
     - Nome: JAVA_HOME
     - Valor: C:\Program Files\Eclipse Adoptium\jdk-17.0.15.8-hotspot (Verifique o caminho exato na sua máquina, a versão pode variar um pouco).
   - SPARK_HOME:
     - Nome: SPARK_HOME
     - Valor: C:\spark\spark-4.0.0-bin-hadoop3
   - HADOOP_HOME:
     - Nome: HADOOP_HOME
     - Valor: C:\hadoop
   - PYSPARK_PYTHON:
     - Nome: PYSPARK_PYTHON
     - Valor: python (Isso instrui o Spark a usar o Python que estiver ativo no terminal, respeitando seu ambiente virtual venv).
3. Edite a Variável Path do Sistema:
   - Na mesma seção de "Variáveis de sistema", encontre e selecione a variável Path e clique em Editar....
   - Clique em Novo e adicione estas três entradas, uma de cada vez:
     - %SPARK_HOME%\bin
     - %HADOOP_HOME%\bin
     - %JAVA_HOME%\bin
   - Salve tudo clicando em OK em todas as janelas.

### Passo 5: Permissões e Reinicialização

1. Conceda Permissões (Modo Administrador):
   - Abra o Prompt de Comando (cmd) como Administrador.
   - Execute os seguintes comandos (se o seu Windows for em português, use Usuários; se for em inglês, use Users):
     - icacls C:\hadoop /grant Usuários:F /T
     - icacls C:\spark /grant Usuários:F /T
2. REINICIE O COMPUTADOR: Este passo é crucial e força o Windows a carregar todas as novas configurações e permissões.

### Passo 6: Verificação Final

1. Após reiniciar, abra um novo terminal (PowerShell ou CMD)
2. Navegue para a pasta do seu projeto (use um caminho sem espaços, ex: C:\projetos\meu_projeto).
3. Ative seu ambiente virtual: .\venv\Scripts\Activate.ps1.
4. Execute o spark-submit com seu script. Exemplo:
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 seu_script_spark.py
```
5. Se tudo foi configurado corretamente, o Spark deverá iniciar sem erros. Parabéns, seu ambiente está pronto!

## Configurando Delta Lake para pipeline de dados históricos

1. Baixe a versão mais recente (42.7.7) do Driver JDBC do PostgreSQL: https://jdbc.postgresql.org/download/ 
2. Coloque o arquivo em uma pasta fácil de achar (sugestão: `C:\spark\jdbc\postgresql-42.7.7.jar` no Windows)
3. Adicione o caminho para o arquivo às suas variáveis de ambiente:
```config
JDBC_JAR_PATH="C:/spark/jdbc/postgresql-42.7.7.jar" # exemplo colocandona pasta sugerida
```
4. Rode comandos do spark com DeltaLake com a seguinte linha (até o momento o arquivo `run_pipeline_spark_batch.py` é o mais atualizado no desenvolvimento do pipeline de métricas históricas):
```bash
spark-submit --packages io.delta:delta-spark_2.13:4.0.0 --jars C:\spark\jdbc\postgresql-42.7.7.jar run_pipeline_spark_bash.py
```
Essa linha roda os comandos Spark enquanto faz o download da versão compatível do DeltaLake com a versão do Spark utilizada no projeto.

- Obs: No Windows é comum ocorrer o erro: `java.io.IOException: Failed to delete: C:\Users\daniel\AppData\Local\Temp\spark-34a12dad-52f8-4361-a1cc-d76f007d3191\userFiles-0df6c170-3d90-4c6c-8c8b-4660680ee9b3\postgresql-42.7.7.jar` após rodar esses e outros comandos Spark. Não se preocupe, isso não só é normal como não impacta no projeto e o Windows automaticamente deletará esses arquivos quando o computador for reiniciado.


# Trabalho A2 de Computação Escalável 
## Objetivo
Criar um pipeline escalável para processamento de dados utilizando os mecanismos apresentados na disciplina.

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
	- tipo_evento: String (e.g., "visualizacao_produto", "produto_adicionado_carrinho", "carrinho_criado", "checkout_concluido", "login")
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
