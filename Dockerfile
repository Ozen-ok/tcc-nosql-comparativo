# 1. Imagem base: Começamos com um ambiente Python limpo e leve.
FROM python:3.10-slim-bookworm

# Atualiza os pacotes do sistema para corrigir vulnerabilidades conhecidas
RUN apt-get update && apt-get upgrade -y && apt-get clean

# 2. Define o diretório de trabalho principal dentro do contêiner.
WORKDIR /app

# 3. Copia e instala as dependências (a "receita de bolo").
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copia os dados do seu projeto para dentro do contêiner.
COPY ./data /app/data

# 5. Copia os assets (imagens, logos, etc.) para o contêiner.
COPY ./assets /app/assets

COPY ./testes /app/testes

# 6. Copia todo o seu código-fonte para dentro do contêiner.
COPY ./src /app/src


# 7. Comando padrão para iniciar o servidor da API FastAPI quando o contêiner subir.
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]