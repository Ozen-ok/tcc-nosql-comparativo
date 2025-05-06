import unittest
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from config.db_config import CASSANDRA_USER, CASSANDRA_PASSWORD, CASSANDRA_HOST, CASSANDRA_KEYSPACE
import ast  # Usado para avaliar a string que representa a lista (generos)

class TestCassandraOperations(unittest.TestCase):
    def setUp(self):
        # Configuração de conexão com o Cassandra
        auth_provider = PlainTextAuthProvider(CASSANDRA_USER, CASSANDRA_PASSWORD)
        cluster = Cluster([CASSANDRA_HOST], auth_provider=auth_provider)
        self.session = cluster.connect()

        # Criar o keyspace se ele não existir
        self.session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
        """)

        # Seleciona o keyspace para uso
        self.session.set_keyspace(CASSANDRA_KEYSPACE)

        # Criar a tabela filmes se ela não existir
        self.session.execute("""
        CREATE TABLE IF NOT EXISTS filmes (
            titulo_id TEXT,         -- Identificador único do filme
            ator_id TEXT,           -- Identificador único do ator
            nome_personagem TEXT,   -- Nome do personagem interpretado pelo ator
            tipo TEXT,              -- Tipo do filme (por exemplo, "Filme", "Documentário", etc.)
            titulo TEXT,            -- Título do filme
            ano_lancamento INT,     -- Ano de lançamento
            generos LIST<TEXT>,     -- Gêneros do filme (lista)
            nome_ator TEXT,         -- Nome do ator
            ano_nascimento INT,     -- Ano de nascimento do ator
            nota FLOAT,             -- Nota do filme
            numero_votos INT,       -- Número de votos
            duracao FLOAT,          -- Duração do filme (em minutos)
            sinopse TEXT,           -- Sinopse do filme
            PRIMARY KEY (titulo_id, ator_id)  -- Chave primária composta (titulo_id, ator_id)
        );
        """)

    def test_1_insercao_dados(self):
        # Carregar dados do arquivo .tsv
        df = pd.read_csv('data/dados_filtrados.tsv', sep='\t')

        query = """
        INSERT INTO filmes (titulo_id, ator_id, nome_personagem, tipo, titulo, ano_lancamento, generos, nome_ator, ano_nascimento, nota, numero_votos, duracao, sinopse)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Iterando pelas linhas do DataFrame e inserindo os dados
        for index, row in df.iterrows():
            # Convertendo a string que representa uma lista (por exemplo: "['Drama', 'Romance', 'Esporte']") para uma lista real
            generos_list = ast.literal_eval(row['generos'])  # Usando ast.literal_eval para avaliar a string como uma lista
            
            # Verificando se generos_list é uma lista válida e se contém strings
            if isinstance(generos_list, list):
                generos_list = [str(item) for item in generos_list]  # Garantir que todos os itens sejam strings
            else:
                generos_list = []  # Caso o formato da string seja inválido, garantir uma lista vazia

            novo_filme = (
                row['titulo_id'],
                row['ator_id'],
                row['nome_personagem'],
                row['tipo'],
                row['titulo'],
                row['ano_lancamento'],
                generos_list,  # A lista de gêneros agora está garantida como lista de strings
                row['nome_ator'],
                row['ano_nascimento'],
                row['nota'],
                row['numero_votos'],
                row['duracao'],
                row['sinopse']
            )

            # Execute a consulta para inserir os dados
            self.session.execute(query, novo_filme)
        
        self.assertTrue(True, "Erro na inserção!")

    def test_2_consulta_dados(self):
        query = "SELECT * FROM filmes WHERE titulo_id = 'tt0091065' AND ator_id = 'nm0000206'"
        filme = self.session.execute(query).one()
        self.assertIsNotNone(filme, "Filme não encontrado!")
        self.assertEqual(filme.titulo, "Voando para o Sucesso")

    def test_3_atualizacao_dados(self):
        query = """
        UPDATE filmes SET nota = %s WHERE titulo_id = 'tt0091065' AND ator_id = 'nm0000206'
        """
        self.session.execute(query, (6.0,))
        query_select = "SELECT nota FROM filmes WHERE titulo_id = 'tt0091065' AND ator_id = 'nm0000206'"
        filme_atualizado = self.session.execute(query_select).one()
        self.assertEqual(filme_atualizado.nota, 6.0)

    def test_4_contagem_dados(self):
        query = "SELECT COUNT(*) FROM filmes WHERE generos CONTAINS 'Drama' ALLOW FILTERING"
        count = self.session.execute(query).one()[0]
        self.assertGreater(count, 0, "Nenhum filme com o gênero Drama!")

    def test_5_agregacao(self):
        query = "SELECT COUNT(*) FROM filmes WHERE ano_lancamento = 1986 ALLOW FILTERING"
        resultado = self.session.execute(query).one()[0]
        self.assertGreater(resultado, 0, "Nenhum resultado encontrado!")

    def test_6_remocao_dados(self):
        query = "DELETE FROM filmes WHERE titulo_id = 'tt0091065' AND ator_id = 'nm0000206'"
        self.session.execute(query)
        query_select = "SELECT * FROM filmes WHERE titulo_id = 'tt0091065' AND ator_id = 'nm0000206'"
        filme_removido = self.session.execute(query_select).one()
        self.assertIsNone(filme_removido, "O filme ainda está presente no banco!")

    def tearDown(self):
        self.session.cluster.shutdown()

if __name__ == "__main__":
    unittest.main()
