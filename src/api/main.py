# src/api/main.py
from fastapi import FastAPI
# Remova ou comente as importações dos routers específicos se for usar SÓ o genérico por agora
# from src.databases.cassandra import api as cassandra_api
# from src.databases.mongo import api as mongo_api
# from src.databases.neo4j import api as neo4j_api
# from src.databases.redis import api as redis_api

# Importe o novo router genérico
from src.api.routers.v1 import generic_router # Certifique-se que o caminho está correto

app = FastAPI(
    title="IMDB NoSQL API Genérica",
    description="API para testes com MongoDB, Cassandra, Neo4j e Redis, usando endpoints genéricos.",
    version="1.0.0"
)

# Inclui o router genérico com um prefixo /api/v1
app.include_router(generic_router.router, prefix="/api/v1", tags=["Operações Genéricas v1"])

# Se quiser manter os routers específicos para teste direto (NÃO SERÃO USADOS PELO STREAMLIT REATORADO):
# print("Incluindo routers específicos para teste direto...")
# from src.databases.mongo import api as mongo_api_specific
# from src.databases.cassandra import api as cassandra_api_specific
# app.include_router(mongo_api_specific.router)
# app.include_router(cassandra_api_specific.router)
# ... e os outros ...

# Adicione um endpoint raiz simples para verificar se a API está no ar
@app.get("/", tags=["Root"])
async def read_root():
    return {"message": "Bem-vindo à API Genérica IMDB NoSQL!"}