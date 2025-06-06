import os
from pathlib import Path
from dotenv import load_dotenv
#src/core/db_config.py

# Carrega variáveis do .env na raiz do projeto
env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=env_path)

# Variáveis de ambiente (com castings necessários)
MONGO_HOST = os.getenv("MONGO_INITDB_ROOT_HOST")
MONGO_PORT = int(os.getenv("MONGO_INITDB_ROOT_PORT", "27017"))
MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USER")
MONGO_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
MONGO_DB_NAME = os.getenv("MONGO_INITDB_ROOT_DB_NAME")

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_USER = os.getenv("CASSANDRA_USER")
CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE")

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))