import pandas as pd
from pymongo import MongoClient
import ast

def importar_mongodb(uri="mongodb://localhost:27017", db_name="imdb", collection_name="filmes"):
    # Leitura do TSV
    df = pd.read_csv("data/dados_filtrados.tsv", sep="\t")

    # Conexão com MongoDB
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]

    # Limpa a coleção antes de inserir novos dados (opcional)
    collection.delete_many({})

    registros = []
    for _, row in df.iterrows():
        doc = {
            "ator_id": row["ator_id"],
            "titulo_id": row["titulo_id"],
            "nome_personagem": row["nome_personagem"],
            "tipo": row["tipo"],
            "titulo": row["titulo"],
            "ano_lancamento": int(row["ano_lancamento"]),
            "generos": ast.literal_eval(row["generos"]),  # converte string para lista
            "nome_ator": row["nome_ator"],
            "ano_nascimento": int(row["ano_nascimento"]) if pd.notna(row["ano_nascimento"]) else None,
            "nota": float(row["nota"]) if pd.notna(row["nota"]) else None,
            "numero_votos": int(row["numero_votos"]) if pd.notna(row["numero_votos"]) else 0,
            "duracao": float(row["duracao"]) if pd.notna(row["duracao"]) else None,
            "sinopse": row["sinopse"]
        }
        registros.append(doc)

    # Inserção em lote
    collection.insert_many(registros)
    print(f"Inseridos {len(registros)} documentos no MongoDB.")

if __name__ == "__main__":
    importar_mongodb()
