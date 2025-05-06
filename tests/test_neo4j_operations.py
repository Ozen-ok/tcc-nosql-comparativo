import unittest
import pandas as pd
from neo4j import GraphDatabase
import ast
from config.db_config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

class TestNeo4jOperations(unittest.TestCase):
    def setUp(self):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    def test_1_insercao_dados(self):
        df = pd.read_csv("data/dados_filtrados.tsv", sep="\t")

        with self.driver.session() as session:
            for _, row in df.iterrows():
                generos = ast.literal_eval(row['generos']) if isinstance(row['generos'], str) else []
                session.run("""
                    MERGE (a:Ator {id: $ator_id})
                    SET a.nome = $nome_ator, a.ano_nascimento = $ano_nascimento
                    MERGE (f:Filme {id: $titulo_id})
                    SET f.titulo = $titulo, f.ano_lancamento = $ano_lancamento, f.tipo = $tipo,
                        f.generos = $generos, f.nota = $nota, f.numero_votos = $numero_votos,
                        f.duracao = $duracao, f.sinopse = $sinopse
                    MERGE (a)-[r:ATUOU_EM {personagem: $nome_personagem}]->(f)
                """, {
                    "ator_id": row['ator_id'],
                    "nome_ator": row['nome_ator'],
                    "ano_nascimento": row['ano_nascimento'],
                    "titulo_id": row['titulo_id'],
                    "titulo": row['titulo'],
                    "ano_lancamento": row['ano_lancamento'],
                    "tipo": row['tipo'],
                    "generos": generos,
                    "nota": row['nota'],
                    "numero_votos": row['numero_votos'],
                    "duracao": row['duracao'],
                    "sinopse": row['sinopse'],
                    "nome_personagem": row['nome_personagem']
                })

        self.assertTrue(True)

    def test_2_consulta_filme_por_ator(self):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (a:Ator {id: 'nm0000206'})-[:ATUOU_EM]->(f:Filme {id: 'tt0091065'})
                RETURN f.titulo AS titulo
            """)
            record = result.single()
            self.assertIsNotNone(record)
            self.assertEqual(record["titulo"], "Voando para o Sucesso")

    def test_3_relacionamento_entre_atores(self):
        with self.driver.session() as session:
            result = session.run("""
                MATCH (a1:Ator)-[:ATUOU_EM]->(f:Filme)<-[:ATUOU_EM]-(a2:Ator)
                WHERE a1.id <> a2.id
                RETURN a1.nome AS ator1, a2.nome AS ator2, f.titulo AS filme
                LIMIT 1
            """)
            record = result.single()
            self.assertIsNotNone(record)

    def test_4_remocao_de_filme(self):
        with self.driver.session() as session:
            session.run("""
                MATCH (a:Ator {id: 'nm0000206'})-[r:ATUOU_EM]->(f:Filme {id: 'tt0091065'})
                DELETE r, f
            """)
            result = session.run("""
                MATCH (f:Filme {id: 'tt0091065'}) RETURN f
            """)
            record = result.single()
            self.assertIsNone(record)

    def test_5_contagem_filmes(self):
        with self.driver.session() as session:
            result = session.run("MATCH (f:Filme) RETURN count(f) AS total")
            count = result.single()["total"]
            self.assertGreater(count, 0)

    def test_6_media_notas(self):
        with self.driver.session() as session:
            result = session.run("MATCH (f:Filme) RETURN avg(f.nota) AS media")
            media = result.single()["media"]
            self.assertIsNotNone(media)

    def tearDown(self):
        self.driver.close()

if __name__ == "__main__":
    unittest.main()
