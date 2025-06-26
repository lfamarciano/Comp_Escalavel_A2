# --- Estágio 1: Base Python ---
# Uma base comum e leve com Python para todos os serviços.
FROM python:3.11-slim-bookworm AS base

WORKDIR /app

# --- Estágio 2: Java Base ---
# Estágio em cima do python que adiciona a OpenJDK
# Como é caro de fazre build, vai ser feito um cache
FROM base AS java-base
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk curl && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# --- Estágio 3: Aplicação Python Final ---
# Imagem final para os serviços que precisam só do python
FROM base AS app-python
COPY requirements.txt .
# Usando uv para instalação mais rápida
RUN pip install uv
RUN uv pip install --system --no-cache-dir -r requirements.txt

# Copia os ficheiros da aplicação primeiro para aproveitar o cache do Docker
COPY ./src .

CMD ["echo", "Imagem 'app-python' pronta. Especifiaque um comando com docker compose."]


# --- Estágio 4: Aplicação Spark Final ---
# Imagem final com Java e Python
FROM java-base AS app-spark

# Instalando Spark (que também terá um cache)
WORKDIR /opt/spark
ENV SPARK_VERSION=4.0.0
ENV HADOOP_VERSION=3
ENV POSTGRES_JDBC_VERSION=42.7.7

# Usando URL do CDN, mais rápido e confiável
RUN curl -fSLO "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" --strip-components=1 && \
rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
# Adicionando drive JDBC do PostgreSQL
RUN curl -fSL -o "/opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar" "https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar"
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
COPY log4j.properties ./conf/log4j.properties

# Voltando para app
WORKDIR /app
COPY requirements.txt .
RUN pip install uv
RUN uv pip install --system --no-cache-dir -r requirements.txt

COPY ./src .

CMD ["echo", "Imagem 'app-spark' pronta. Especifique um comando com docker compose."]
