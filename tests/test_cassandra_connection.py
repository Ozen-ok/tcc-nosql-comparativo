import unittest
from app.services.cassandra_service import get_cassandra_connection, close_cassandra_connection

class TestCassandraConnection(unittest.TestCase):
    def test_cassandra_connection(self):
        session, cluster = get_cassandra_connection()  # Obtém tanto a sessão quanto o cluster
        self.assertIsNotNone(session)  # Verifica se a conexão com o Cassandra foi feita
        close_cassandra_connection(cluster)  # Fecha a conexão após o teste

if __name__ == "__main__":
    unittest.main()
