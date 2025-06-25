# Estágio 1: Começa com uma imagem base oficial do Python 3.11.
# Escolhemos a variante 'slim-bookworm' por ser leve e estável.
FROM python:3.11-slim-bookworm AS base

# Define variáveis de ambiente para evitar prompts interativos durante a instalação
ENV DEBIAN_FRONTEND=noninteractive

# Instala o Java 17 (JDK) e o curl, que são necessários para o Spark
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk curl && \
    rm -rf /var/lib/apt/lists/*

# Define o JAVA_HOME para que o Spark o encontre
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Estágio 2: Instalação do Spark
FROM base AS spark-ready

WORKDIR /opt/spark

# Define as versões do Spark e do Hadoop
ENV SPARK_VERSION=4.0.0
ENV HADOOP_VERSION=3

# Baixa e descompacta o Spark
RUN curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xz --strip-components=1

# Define as variáveis de ambiente do Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Estágio 3: Aplicação Final
FROM spark-ready

WORKDIR /app

# Copia os ficheiros da sua aplicação
COPY ./src .
COPY requirements.txt .

# CORREÇÃO: Usa o comando pip install padrão, que é universalmente compatível.
# A opção --no-cache-dir garante que a imagem final não contenha o cache do pip.
RUN pip install --no-cache-dir -r requirements.txt

# Comando padrão que será sobreposto pelo docker-compose
CMD ["echo", "Imagem pronta. Especifique um comando no docker-compose.yml para executar uma aplicação."]
