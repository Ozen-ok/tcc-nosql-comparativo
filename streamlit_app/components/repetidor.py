import pandas as pd
import os

MONGO_API_URL = "http://localhost:8000/mongo"
REDIS_API_URL = "http://localhost:8000/redis"
CASSANDRA_API_URL = "http://localhost:8000/cassandra"
NEO4J_API_URL = "http://localhost:8000/neo4j"

API_URLS = {
    "mongo": MONGO_API_URL,
    "redis": REDIS_API_URL,
    "cassandra": CASSANDRA_API_URL,
    "neo4j": NEO4J_API_URL,
}

def preparar_dados_filmes(filmes):
    if not filmes:
        return []

    # Converte os filmes para um DataFrame
    df = pd.DataFrame(filmes)

    # Converte a coluna 'nota' para float, se ela existir
    if "nota" in df.columns:
        df["nota"] = pd.to_numeric(df["nota"], errors="coerce")  # Converte para float, valores inválidos se tornam NaN

    # Adiciona a URL do poster
    df["poster_url"] = df["titulo_id"].apply(lambda tid: f"assets/imagens/posters/{tid}.jpg")
    
    # Converte de volta para um dicionário
    return df.to_dict(orient="records")

def verificar_titulos_sem_imagem(filmes):
    if not filmes:
        return []

    df = pd.DataFrame(filmes)

    # Caminho base onde as imagens deveriam estar
    base_path = "assets/imagens/posters"

    # Função auxiliar que verifica se o arquivo de imagem existe
    def imagem_existe(titulo_id):
        caminho_imagem = os.path.join(base_path, f"{titulo_id}.jpg")
        return os.path.isfile(caminho_imagem)

    # Filtra os títulos que **não** têm imagem
    df_sem_imagem = df[~df["titulo_id"].apply(imagem_existe)]
    return df_sem_imagem["titulo_id"].tolist()

