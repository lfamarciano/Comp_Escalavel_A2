# --- Estágio 1: Base Python ---
# Uma base comum e leve com Python para todos os serviços.
FROM python:3.11-slim-bookworm AS base

WORKDIR /app

# Copia os ficheiros da aplicação primeiro para aproveitar o cache do Docker
COPY ./src .
COPY requirements.txt .


# --- Estágio 2: Aplicações Python Puras ---
# Esta é a imagem final para os serviços que SÓ precisam de Python.
FROM base AS app-python

# Instala as dependências a partir do requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Comando padrão (será sobreposto pelo docker-compose)
CMD ["echo", "Imagem 'app-python' pronta. Especifique um comando no docker-compose.yml."]


# --- Estágio 3: Aplicações Spark ---
# Esta é a imagem final para os serviços que precisam do ambiente Spark completo.
FROM base AS app-spark

# Instala o Java (requisito do Spark)
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk curl && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Instala o Spark
WORKDIR /opt/spark
ENV SPARK_VERSION=4.0.0
ENV HADOOP_VERSION=3
RUN curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xz --strip-components=1
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Volta para o diretório da aplicação e instala as dependências Python
WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt

# Comando padrão (será sobreposto pelo docker-compose)
CMD ["echo", "Imagem 'app-spark' pronta. Especifique um comando no docker-compose.yml."]