from pymongo.collection import Collection

# INSERÇÕES -----------------------------------------------

def inserir_filme(collection: Collection, filme: dict):
    return collection.insert_one(filme)

def inserir_ator(collection: Collection, ator: dict):
    return collection.insert_one(ator)

def inserir_elenco(collection: Collection, relacao: dict):
    return collection.insert_one(relacao)

# CONSULTAS -----------------------------------------------

def buscar_filmes_por_genero(collection: Collection, generos: list):
    """
    Busca filmes que tenham pelo menos todos os gêneros informados.
    """
    query = {"generos": {"$all": generos}}
    return list(collection.find(query))

def buscar_filmes_avancado(collection: Collection, generos: list, ano_min: int, nota_min: float):
    """
    Consulta avançada combinando múltiplos filtros, incluindo o filtro de tipo.
    """
    query = {
        "generos": {"$all": generos},  # Gêneros devem conter todos os valores fornecidos
        "ano_lancamento": {"$gte": ano_min},  # Ano de lançamento maior ou igual ao ano mínimo
        "nota": {"$gte": nota_min},  # Nota maior ou igual à nota mínima
    }
    
    filmes = list(collection.find(query))

    return filmes


# ATUALIZAÇÃO ---------------------------------------------

def atualizar_nota_filme(collection: Collection, titulo_id: str, nova_nota: float):
    return collection.update_one(
        {"titulo_id": titulo_id},
        {"$set": {"nota": nova_nota}}
    )

# REMOÇÃO -------------------------------------------------

def remover_filme(collection: Collection, titulo_id: str):
    return collection.delete_one({"titulo_id": titulo_id})

# AGREGAÇÃO / ANÁLISE -------------------------------------

def contar_filmes_por_ano(collection: Collection):
    pipeline = [
        {"$group": {"_id": "$ano_lancamento", "quantidade": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    return list(collection.aggregate(pipeline))

def media_notas_por_genero(collection: Collection):
    pipeline = [
        {"$unwind": "$generos"},
        {"$group": {
            "_id": "$generos",
            "soma_nota": {"$sum": "$nota"},
            "contagem": {"$sum": 1}
        }},
        {"$project": {
            "_id": 0,
            "genero": "$_id",
            "media_nota": {"$divide": ["$soma_nota", "$contagem"]}
        }},
        {"$sort": {"media_nota": -1}}
    ]
    return list(collection.aggregate(pipeline))

from config.db_config import get_mongo_client

# Inserir Filme
def inserir_filme3(filme):
    client = get_mongo_client()
    try:
        db = client['imdb_db']
        colecao_filmes = db['filmes']
        colecao_filmes.insert_one(filme)
    except Exception as e:
        print(f"Erro ao inserir o filme: {e}")

# Inserir Ator
def inserir_ator3(ator):
    client = get_mongo_client()
    try:
        db = client['imdb_db']
        colecao_atores = db['atores']
        colecao_atores.insert_one(ator)
    except Exception as e:
        print(f"Erro ao inserir o ator: {e}")

# Inserir Elenco (Relação Ator - Filme)
def inserir_elenco3(elenco):
    client = get_mongo_client()
    try:
        db = client['imdb_db']
        colecao_elenco = db['elenco']
        colecao_elenco.insert_one(elenco)
    except Exception as e:
        print(f"Erro ao inserir o elenco: {e}")
