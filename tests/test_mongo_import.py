import unittest
import pandas as pd
from pymongo import MongoClient
from config.db_config import MONGO_INITDB_ROOT_USER, MONGO_INITDB_ROOT_PASSWORD, MONGO_INITDB_ROOT_HOST, MONGO_INITDB_ROOT_PORT
from urllib.parse import quote_plus

class TestMongoImport(unittest.TestCase):
    
    def setUp(self):
        # Codificando usuário e senha para evitar erros de URL
        user = quote_plus(MONGO_INITDB_ROOT_USER)
        password = quote_plus(MONGO_INITDB_ROOT_PASSWORD)

        # Banco de autenticação é 'admin' para usuário root
        uri = f"mongodb://{user}:{password}@{MONGO_INITDB_ROOT_HOST}:{MONGO_INITDB_ROOT_PORT}/admin"

        # Conexão com o MongoDB
        self.client = MongoClient(uri)
        self.db = self.client["testdb"]  # ou qualquer nome do seu banco de dados de trabalho
        self.collection = self.db["filmes"]

        # Carregar dados do arquivo TSV dentro do container Docker
        self.dados = pd.read_csv('data/dados_filtrados.tsv', sep='\t')  # Usando o caminho do arquivo dentro do container

        # Importar os dados para o MongoDB
        self.importar_dados()

    def importar_dados(self):
        """Função para importar os dados do TSV para o MongoDB"""
        for _, row in self.dados.iterrows():
            document = {
                "ator_id": row['ator_id'],
                "titulo_id": row['titulo_id'],
                "nome_personagem": row['nome_personagem'],
                "tipo": row['tipo'],
                "titulo": row['titulo'],
                "ano_lancamento": row['ano_lancamento'],
                "generos": row['generos'],  # Lista de gêneros já no formato correto
                "nome_ator": row['nome_ator'],
                "ano_nascimento": row['ano_nascimento'],
                "nota": row['nota'],
                "numero_votos": row['numero_votos'],
                "duracao": row['duracao'],
                "sinopse": row['sinopse']
            }
            self.collection.insert_one(document)

    def test_dados_foram_importados(self):
        """Teste para garantir que os dados foram importados com sucesso"""
        count = self.collection.count_documents({})
        self.assertGreater(count, 0, "Nenhum documento foi importado!")  # Verifica se existem documentos na coleção

    def tearDown(self):
        """Fechar a conexão após os testes"""
        self.client.close()

if __name__ == "__main__":
    unittest.main()
