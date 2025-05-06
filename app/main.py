import pandas as pd
from app.crud.mongo_crud import insert_movie, insert_actor
from app.models.mongo_models import Movie, Actor
import ast  # Para converter string para lista

# Carregar os dados do arquivo TSV
df = pd.read_csv("data/dados_filtrados.tsv", sep="\t")

# Inserir dados no MongoDB
for index, row in df.iterrows():
    # Garantir que 'generos' seja uma lista
    try:
        generos = ast.literal_eval(row["generos"])  # Converte a string para lista
    except:
        generos = []  # Em caso de erro, atribui uma lista vazia
    
    # Criar e inserir o filme
    movie = Movie(
        titulo_id=row["titulo_id"],
        nome_personagem=row["nome_personagem"],
        tipo=row["tipo"],
        titulo=row["titulo"],
        ano_lancamento=row["ano_lancamento"],
        generos=generos,
        nome_ator=row["nome_ator"],
        ano_nascimento=row["ano_nascimento"],
        nota=row["nota"],
        numero_votos=row["numero_votos"],
        duracao=row["duracao"],
        sinopse=row["sinopse"]
    )
    insert_movie(movie)
    
    # Criar e inserir o ator
    actor = Actor(
        ator_id=row["ator_id"],
        nome_ator=row["nome_ator"],
        ano_nascimento=row["ano_nascimento"]
    )
    insert_actor(actor)

print("Dados inseridos no MongoDB com sucesso!")
