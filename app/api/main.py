from fastapi import FastAPI
from app.api import mongo_api, cassandra_api, neo4j_api
app = FastAPI(
    title="IMDB NoSQL API",
    description="API para testes com MongoDB, Cassandra, Neo4j e Redis",
    version="1.0.0"
)

app.include_router(mongo_api.router)
app.include_router(cassandra_api.router)  
app.include_router(neo4j_api.router)  
