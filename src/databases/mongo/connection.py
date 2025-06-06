from pymongo import MongoClient
from typing import Optional
from src.core.db_config import (
    MONGO_USER,
    MONGO_PASSWORD,
    MONGO_HOST,
    MONGO_PORT,
    MONGO_DB_NAME
)

def _get_mongo_uri() -> str:
    return f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/admin"


def get_mongo_client() -> MongoClient:
    """Retorna um cliente MongoDB conectado."""
    uri = _get_mongo_uri()
    try:
        client = MongoClient(uri)
        # Força a conexão para detectar erros na inicialização
        client.admin.command('ping')
        return client
    except Exception as e:
        raise ConnectionError(f"Falha ao conectar no MongoDB: {e}")


def get_mongo_db() -> Optional[MongoClient]:
    """Retorna o banco de dados MongoDB especificado."""
    client = get_mongo_client()
    return client[MONGO_DB_NAME] if client else None