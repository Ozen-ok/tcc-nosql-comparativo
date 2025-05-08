import pandas as pd
from app.crud.mongo_crud import inserir_filme3, inserir_ator3, inserir_elenco3
from app.backup_antigo.models.mongo_models import Filme, Ator, Elenco
import ast  # Para converter string para lista

# Carregar os dados dos arquivos TSV
filmes_df = pd.read_csv("data/filmes.tsv", sep="\t")
atores_df = pd.read_csv("data/atores.tsv", sep="\t")
elenco_df = pd.read_csv("data/elenco.tsv", sep="\t")

# Inserir dados no MongoDB

# Inserir filmes
for _, linha in filmes_df.iterrows():
    try:
        generos = ast.literal_eval(linha["generos"])  # Converte a string para lista
    except (ValueError, SyntaxError):  # Captura erros específicos
        generos = []  # Em caso de erro, atribui uma lista vazia
    
    # Criar o filme
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
    
    # Converter a instância do modelo para dicionário e inserir
    inserir_filme3(filme.model_dump())  # Converte para dicionário e passa para a função

# Inserir atores
for _, linha in atores_df.iterrows():
    ator = Ator(
        ator_id=linha["ator_id"],
        nome_ator=linha["nome_ator"],
        ano_nascimento=linha["ano_nascimento"]
    )
    
    # Converter a instância do modelo para dicionário e inserir
    inserir_ator3(ator.model_dump())  # Converte para dicionário e passa para a função

# Inserir relações de elenco (ator-filme)
for _, linha in elenco_df.iterrows():
    # Criar a relação de elenco com o modelo Pydantic
    elenco = Elenco(
        ator_id=linha["ator_id"],
        titulo_id=linha["titulo_id"],
        nome_personagem=linha["nome_personagem"],
    )
    
    # Converter a instância do modelo para dicionário e inserir
    inserir_elenco3(elenco.model_dump())  # Converte para dicionário e passa para a função

print("✅ Dados inseridos no MongoDB com sucesso!")
