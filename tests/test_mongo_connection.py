# tests/test_mongo_connection.py
import unittest
from app.services.mongodb_service import get_mongo_connection, close_mongo_connection

class TestMongoConnection(unittest.TestCase):
    def test_mongo_connection(self):
        db, client = get_mongo_connection()  # Obtém tanto o banco quanto o cliente
        self.assertIsNotNone(db)  # Verifica se a conexão com o banco foi feita
        close_mongo_connection(client)  # Fecha a conexão após o teste

if __name__ == "__main__":
    unittest.main()
