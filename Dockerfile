# Estágio 1: Começa com uma imagem base oficial do Python 3.11.
# Escolhemos a variante 'slim-bookworm' por ser leve e estável.
FROM python:3.11-slim-bookworm AS python-base
ENV PYTHONDONWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Estágio 2: base para instalação de dependência python com uv
FROM python-base AS builder
WORKDIR /app

# Instalando uv com pip
RUN pip install uv

# Copiando requirements para utilizar caching
COPY requirements.txt .

# Instalando dependências com uv
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system --no-cache -r requirements.txt

# --- Imagem 1: imagem python leve so para aplicações com python ---
FROM builder AS app-python
WORKDIR /app

# Copiando pacotes instalados no estágio construtor
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copiando código da aplicação em si
COPY ./src .

# --- Imagem 2: imagem com tudo, incluindo Spark ---
FROM builder AS app-spark
WORKDIR /app

# Instalando Java e curl
ENV DEBIAN_FRONTEND=noninteractive
# Instala o Java 17 (JDK) e o curl, que são necessários para o Spark
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk curl && \
    rm -rf /var/lib/apt/lists/*

# Define o JAVA_HOME para que o Spark o encontre
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# Define as versões do Spark e do Hadoop
ENV SPARK_VERSION=4.0.0
ENV HADOOP_VERSION=3

# Define as variáveis de ambiente do Spark
ENV SPARK_HOME=/opt/spark

RUN mkdir -p $SPARK_HOME

# Baixa e descompacta o Spark
RUN curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xz --strip-components=1 -C $SPARK_HOME

ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Copiando dependência e código
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY ./src .

# Comando padrão que será sobreposto pelo docker-compose
CMD ["echo", "Imagem pronta. Especifique um comando no docker-compose.yml para executar uma aplicação."]
