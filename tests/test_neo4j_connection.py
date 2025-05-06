import unittest
from app.services.neo4j_service import execute_query_with_driver

class TestNeo4jConnection(unittest.TestCase):
    def test_neo4j_connection(self):
        query = "MATCH (n) RETURN n LIMIT 1"  # Consulta simples
        result = execute_query_with_driver(query)
        self.assertIsNotNone(result)  # Verifica se a consulta retornou algo

if __name__ == "__main__":
    unittest.main()
