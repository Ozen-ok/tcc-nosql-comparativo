import unittest
import redis
import pandas as pd
from config.db_config import REDIS_HOST, REDIS_PORT, REDIS_DB
import ast

class TestRedisOperations(unittest.TestCase):
    def setUp(self):
        # Configuração de conexão com o Redis
        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

        # Verificar se a conexão foi bem-sucedida
        try:
            self.r.ping()
        except redis.ConnectionError as e:
            self.fail(f"Erro de conexão com o Redis: {e}")
        if not self.r.ping():
            raise ConnectionError("Não foi possível conectar ao Redis!")

        # Carregar dados com pandas
        caminho_arquivo = 'data/dados_filtrados.tsv'
        self.df = pd.read_csv(caminho_arquivo, sep='\t', encoding='utf-8')

        # Inserir dados no Redis
        self.inserir_dados()

        # Inserção manual para o teste de inserção
        self.r.hset("filme:tt0000001", mapping={
            "titulo": "Filme de Teste",
            "ano_lancamento": "2000",
            "tipo": "filme",
            "nota": "7.0",
            "numero_votos": "1234",
            "duracao": "100",
            "sinopse": "Um filme fictício para teste",
            "nome_personagem": "Teste Personagem"
        })

    def inserir_dados(self):
        for _, row in self.df.iterrows():
            try:
                titulo_id = row['titulo_id']
                ator_id = row['ator_id']
                generos_raw = row['generos']
                try:
                    if isinstance(generos_raw, str) and generos_raw.startswith('['):
                        generos = ast.literal_eval(generos_raw)
                    else:
                        generos = generos_raw.split(',') if pd.notna(generos_raw) else []
                except Exception as e:
                    print(f"Erro ao interpretar gêneros para o filme {row['titulo_id']}: {generos_raw} -> {e}")
                    generos = []

                filme_key = f"filme:{titulo_id}"
                self.r.hset(filme_key, mapping={
                    "titulo": row['titulo'],
                    "ano_lancamento": row['ano_lancamento'],
                    "tipo": row['tipo'],
                    "nota": row['nota'],
                    "numero_votos": row['numero_votos'],
                    "duracao": row['duracao'],
                    "sinopse": row['sinopse'],
                    "nome_personagem": row['nome_personagem']
                })

                for genero in generos:
                    self.r.sadd(f"genero:{genero.strip()}", titulo_id)

                self.r.rpush(f"ator:{ator_id}:filmes", titulo_id)
                self.r.incr(f"ator:{ator_id}:contador")

            except Exception as e:
                print(f"Erro ao inserir dados para {row['titulo']}: {str(e)}")

    def test_1_insercao(self):
        titulo_id = 'tt0000001'
        filme_info = self.r.hgetall(f"filme:{titulo_id}")
        self.assertIsNotNone(filme_info, "Filme não encontrado!")
        self.assertIn(b'titulo', filme_info, "Campo 'titulo' não encontrado no filme!")
        self.assertEqual(filme_info[b'titulo'].decode(), 'Filme de Teste')
        self.assertIn(b'ano_lancamento', filme_info, "Campo 'ano_lancamento' não encontrado!")
        self.assertIn(b'nota', filme_info, "Campo 'nota' não encontrado!")

    def test_2_consulta(self):
        ator_id = 'nm0000206'
        filmes_do_ator = self.r.lrange(f"ator:{ator_id}:filmes", 0, -1)
        self.assertGreater(len(filmes_do_ator), 0, "Nenhum filme encontrado para o ator!")
        filmes_do_ator = [f.decode() for f in filmes_do_ator]
        self.assertIn('tt0095730', filmes_do_ator, "Filme não encontrado para o ator")

    def test_3_atualizacao(self):
        titulo_id = 'tt0000001'
        novo_titulo = 'Filme de Teste Atualizado'
        self.r.hset(f"filme:{titulo_id}", "titulo", novo_titulo)
        filme_info = self.r.hget(f"filme:{titulo_id}", "titulo")
        self.assertEqual(filme_info.decode(), novo_titulo, "Título do filme não foi atualizado corretamente!")

    def test_4_contagem(self):
        ator_id = 'nm0000206'
        contador = self.r.get(f"ator:{ator_id}:contador")
        self.assertIsNotNone(contador, "Contador não encontrado!")
        self.assertGreater(int(contador), 0, "Contador de filmes do ator é zero")

    def test_5_agregacao(self):
        genero = 'Drama'
        filmes_do_genero = self.r.smembers(f"genero:{genero}")
        self.assertGreater(len(filmes_do_genero), 0, "Nenhum filme encontrado no gênero!")
        filmes_do_genero = [f.decode() for f in filmes_do_genero]
        self.assertIn('tt0095853', filmes_do_genero, "Filme não encontrado no gênero Drama")

    def test_6_remocao(self):
        titulo_id = 'tt0000001'
        self.r.delete(f"filme:{titulo_id}")
        filme_info = self.r.hgetall(f"filme:{titulo_id}")
        self.assertEqual(filme_info, {}, "Filme não foi removido corretamente!")

    #def tearDown(self):
    #    self.r.delete("filme:tt0000001")  # Limpa o filme de teste
    #
    #    for _, row in self.df.iterrows():
    #        titulo_id = row['titulo_id']
    #        ator_id = row['ator_id']
    #        self.r.delete(f"filme:{titulo_id}")
    #        self.r.delete(f"ator:{ator_id}:filmes")
    #        self.r.delete(f"ator:{ator_id}:contador")
    #
    #        try:
    #            if isinstance(row['generos'], str) and row['generos'].startswith('['):
    #                generos = ast.literal_eval(row['generos'])
    #            else:
    #                generos = row['generos'].split(',') if pd.notna(row['generos']) else []
    #        except:
    #            generos = []
    #
    #        for genero in generos:
    #            self.r.srem(f"genero:{genero.strip()}", titulo_id)

if __name__ == "__main__":
    unittest.main()
