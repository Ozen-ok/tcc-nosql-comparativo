import pandas as pd
from app.crud.redis_crud import inserir_filme_redis, inserir_ator_redis, inserir_elenco_redis
from app.backup_antigo.models.mongo_models import Filme, Ator, Elenco
import ast  # Para converter string para lista

# Carregar os dados dos arquivos TSV
filmes_df = pd.read_csv("data/filmes.tsv", sep="\t")
atores_df = pd.read_csv("data/atores.tsv", sep="\t")
elenco_df = pd.read_csv("data/elenco.tsv", sep="\t")

# Inserir filmes no Redis
for _, linha in filmes_df.iterrows():
    try:
        generos = ast.literal_eval(linha["generos"])  # Converte a string para lista
    except (ValueError, SyntaxError):  # Captura erros específicos
        generos = []  # Em caso de erro, atribui uma lista vazia

    filme = Filme(
        titulo_id=linha["titulo_id"],
        titulo=linha["titulo"],
        tipo=linha["tipo"],
        ano_lancamento=linha["ano_lancamento"],
        generos=generos,
        nota=linha["nota"],
        numero_votos=linha["numero_votos"],
        duracao=linha["duracao"],
        sinopse=linha["sinopse"]
    )

    inserir_filme_redis(filme.model_dump())  # Insere no Redis

# Inserir atores no Redis
for _, linha in atores_df.iterrows():
    ator = Ator(
        ator_id=linha["ator_id"],
        nome_ator=linha["nome_ator"],
        ano_nascimento=linha["ano_nascimento"]
    )

    inserir_ator_redis(ator.model_dump())  # Insere no Redis

# Inserir relações de elenco no Redis
for _, linha in elenco_df.iterrows():
    elenco = Elenco(
        ator_id=linha["ator_id"],
        titulo_id=linha["titulo_id"],
        nome_personagem=linha["nome_personagem"]
    )

    inserir_elenco_redis(elenco.model_dump())  # Insere no Redis

print("✅ Dados inseridos no Redis com sucesso!")
