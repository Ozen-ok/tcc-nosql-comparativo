import unittest
from pymongo import MongoClient
from config.db_config import MONGO_INITDB_ROOT_USER, MONGO_INITDB_ROOT_PASSWORD, MONGO_INITDB_ROOT_HOST, MONGO_INITDB_ROOT_PORT
from urllib.parse import quote_plus

class TestMongoOperations(unittest.TestCase):
    def setUp(self):
        # Codificando usuário e senha para evitar erros de URL
        user = quote_plus(MONGO_INITDB_ROOT_USER)
        password = quote_plus(MONGO_INITDB_ROOT_PASSWORD)

        # Banco de autenticação é 'admin' para usuário root
        uri = f"mongodb://{user}:{password}@{MONGO_INITDB_ROOT_HOST}:{MONGO_INITDB_ROOT_PORT}/admin"

        # Conexão
        self.client = MongoClient(uri)
        self.db = self.client["testdb"]  # ou qualquer nome do seu banco de dados de trabalho
        self.collection = self.db["filmes"]

    def test_1_insercao_dados(self):
        novo_filme = {
            "titulo": "O Exterminador do Futuro",
            "ano_lancamento": 1984,
            "generos": ["Ação", "Ficção Científica"],
            "nome_ator": "Arnold Schwarzenegger",
            "nota": 8.5
        }
        resultado = self.collection.insert_one(novo_filme)
        self.assertIsNotNone(resultado.inserted_id, "Erro na inserção!")

    def test_2_consulta_dados(self):
        filme = self.collection.find_one({"titulo": "O Exterminador do Futuro"})
        self.assertIsNotNone(filme, "Filme não encontrado!")
        self.assertEqual(filme["titulo"], "O Exterminador do Futuro")

    def test_3_atualizacao_dados(self):
        resultado = self.collection.update_one(
            {"titulo": "O Exterminador do Futuro"},
            {"$set": {"nota": 9.0}}
        )
        self.assertGreater(resultado.matched_count, 0, "Nenhum documento foi atualizado!")
        filme_atualizado = self.collection.find_one({"titulo": "O Exterminador do Futuro"})
        self.assertEqual(filme_atualizado["nota"], 9.0)

    def test_4_contagem_dados(self):
        count = self.collection.count_documents({"generos": "Ação"})
        self.assertGreater(count, 0, "Nenhum documento encontrado com o gênero Ação!")

    def test_5_agregacao(self):
        pipeline = [
            {"$match": {"ano_lancamento": 1984}},
            {"$group": {"_id": "$generos", "total": {"$sum": 1}}}
        ]
        resultado = list(self.collection.aggregate(pipeline))
        self.assertGreater(len(resultado), 0, "Nenhum resultado de agregação encontrado!")

    def test_6_remocao_dados(self):
        resultado = self.collection.delete_one({"titulo": "O Exterminador do Futuro"})
        self.assertGreater(resultado.deleted_count, 0, "Erro ao remover documento!")
        filme_removido = self.collection.find_one({"titulo": "O Exterminador do Futuro"})
        self.assertIsNone(filme_removido, "O filme ainda está presente no banco!")

    def tearDown(self):
        self.client.close()

if __name__ == "__main__":
    unittest.main()
