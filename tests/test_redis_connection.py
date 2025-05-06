import unittest
from app.services.redis_service import get_redis_connection, close_redis_connection

class TestRedisConnection(unittest.TestCase):
    def test_redis_connection(self):
        client = get_redis_connection()  # Obtém o cliente do Redis
        self.assertIsNotNone(client)  # Verifica se a conexão com o Redis foi feita
        close_redis_connection(client)  # Fecha a conexão após o teste

if __name__ == "__main__":
    unittest.main()
