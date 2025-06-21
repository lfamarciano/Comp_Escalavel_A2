# Usa uma imagem Python oficial como base
FROM python:3.11-slim

# Define o diretório de trabalho dentro do contentor
WORKDIR /app

# Copia o ficheiro de dependências para o contentor
COPY requirements.txt .

# Instala as dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o código-fonte da pasta 'src' para o diretório de trabalho do contentor
COPY ./src .

# Comando que será executado quando o contentor iniciar
CMD ["python", "producer.py"]
