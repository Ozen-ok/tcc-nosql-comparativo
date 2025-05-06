import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pathlib import Path
from urllib.parse import quote_plus

# Garante que o .env seja carregado a partir da raiz do projeto
env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path)

# Carregar variáveis do .env
MONGO_HOST = os.getenv("MONGO_INITDB_ROOT_HOST")
MONGO_PORT = os.getenv("MONGO_INITDB_ROOT_PORT")
MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USER")
MONGO_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
MONGO_DB_NAME = os.getenv("MONGO_INITDB_ROOT_DB_NAME")

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT")
CASSANDRA_USER = os.getenv("CASSANDRA_USER")
CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE")

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_DB = os.getenv("REDIS_DB")

MONGO_API_URL = "http://localhost:8000/mongo"
REDIS_API_URL = "http://localhost:8000/redis"
CASSANDRA_API_URL = "http://localhost:8000/cassandra"
NEO4J_API_URL = "http://localhost:8000/neo4j"


def get_mongo_db():
    uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/admin"
    client = MongoClient(uri)
    db = client[MONGO_DB_NAME]
    return db