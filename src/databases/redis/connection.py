import redis
from src.core.db_config import REDIS_HOST, REDIS_PORT, REDIS_DB
# src/databases/redis/connection.py

def get_redis_client() -> redis.Redis:
    """Retorna um cliente Redis."""
    try:
        client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        # Testa a conexão
        client.ping()
        return client
    except Exception as e:
        raise ConnectionError(f"Falha ao conectar no Redis: {e}")