import redis
from config.db_config import REDIS_HOST, REDIS_PORT

def get_redis_connection():
    client = redis.StrictRedis(host=REDIS_HOST, port=int(REDIS_PORT), decode_responses=True)
    return client

def close_redis_connection(client):
    client.close()  # Fecha a conexão com o Redis
