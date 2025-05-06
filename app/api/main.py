from fastapi import FastAPI
from app.api import mongo_api

app = FastAPI(
    title="IMDB NoSQL API",
    description="API para testes com MongoDB, Cassandra, Neo4j e Redis",
    version="1.0.0"
)

# Inclui o roteador sem duplicar prefixo
app.include_router(mongo_api.router)
