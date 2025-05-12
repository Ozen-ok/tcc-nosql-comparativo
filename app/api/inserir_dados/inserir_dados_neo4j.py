import pandas as pd
from app.crud.neo4j_crud import inserir_filme_neo4j, inserir_ator_neo4j, inserir_elenco_neo4j
import ast  # Para converter string de lista (caso necessário)

# Carregar os dados dos arquivos TSV
filmes_df = pd.read_csv("data/filmes.tsv", sep="\t")
atores_df = pd.read_csv("data/atores.tsv", sep="\t")
elenco_df = pd.read_csv("data/elenco.tsv", sep="\t")

# Inserir filmes
for _, row in filmes_df.iterrows():
    try:
        generos = ast.literal_eval(row["generos"]) if isinstance(row["generos"], str) else row["generos"]
        filme = {
            "titulo_id": row["titulo_id"],
            "titulo": row["titulo"],
            "tipo": row["tipo"],
            "ano_lancamento": int(row["ano_lancamento"]),
            "generos": generos,
            "nota": float(row["nota"]),
            "numero_votos": int(row["numero_votos"]),
            "duracao": int(row["duracao"]),
            "sinopse": row["sinopse"]
        }
        inserir_filme_neo4j(filme)
    except Exception as e:
        print(f"Erro ao processar filme: {e}")

# Inserir atores
for _, row in atores_df.iterrows():
    try:
        ator = {
            "ator_id": row["ator_id"],
            "nome_ator": row["nome_ator"],
            "ano_nascimento": int(row["ano_nascimento"])
        }
        inserir_ator_neo4j(ator)
    except Exception as e:
        print(f"Erro ao processar ator: {e}")

# Inserir elenco
for _, row in elenco_df.iterrows():
    try:
        elenco = {
            "ator_id": row["ator_id"],
            "titulo_id": row["titulo_id"],
            "nome_personagem": row["nome_personagem"]
        }
        inserir_elenco_neo4j(elenco)
    except Exception as e:
        print(f"Erro ao processar elenco: {e}")

print("✅ Dados inseridos no Neo4j com sucesso!")
