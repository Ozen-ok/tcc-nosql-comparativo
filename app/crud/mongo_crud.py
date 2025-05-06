import ast
from config.db_config import MONGO_HOST, MONGO_PASSWORD, MONGO_USER, MONGO_PORT
from pymongo import MongoClient
from bson import ObjectId
from typing import List

def convert_to_list(value):
    # Verifica se o valor não é uma lista, então tenta converter
    if isinstance(value, str):
        try:
            value = ast.literal_eval(value)  # Converte a string para lista
            if not isinstance(value, list):  # Verifica se a conversão foi bem-sucedida
                raise ValueError
        except (ValueError, SyntaxError):
            value = []  # Caso a conversão falhe, retorna uma lista vazia
    return value

def get_mongo_client():
    uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/admin"
    client = MongoClient(uri)
    return client

def insert_movie(movie):
    client = get_mongo_client()  # Conectar ao MongoDB
    try:
        db = client['imdb_db']  # Nome do banco de dados
        movie_collection = db['movies']  # Nome da coleção
        movie_collection.insert_one(movie.dict())  # Inserir o filme
    except Exception as e:
        print(f"Erro ao inserir o filme: {e}")

def insert_actor(actor):
    client = get_mongo_client()  # Conectar ao MongoDB
    try:
        db = client['imdb_db']  # Nome do banco de dados
        actor_collection = db['actors']  # Nome da coleção
        actor_collection.insert_one(actor.dict())  # Inserir o ator
    except Exception as e:
        print(f"Erro ao inserir o ator: {e}")

from pymongo.collection import Collection

def inserir_filme(db: Collection, filme: dict):
    return db.insert_one(filme)

from bson import ObjectId

def buscar_filmes_por_genero(collection: Collection, generos: list):
    """
    Busca filmes que tenham pelo menos um dos gêneros informados.
    """
    query = {"generos": {"$all": generos}}  # Garante que pelo menos um gênero seja encontrado
    return collection.find(query)

def atualizar_nota_filme(db: Collection, titulo_id: str, nova_nota: float):
    return db.update_one({"titulo_id": titulo_id}, {"$set": {"nota": nova_nota}})

def remover_filme(db: Collection, titulo_id: str):
    return db.delete_one({"titulo_id": titulo_id})

def contar_filmes_por_ano(db: Collection):
    pipeline = [
        {"$group": {"_id": "$ano_lancamento", "quantidade": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    return list(db.aggregate(pipeline))

def media_notas_por_genero(db: Collection):
    pipeline = [
        {"$unwind": "$generos"},
        {"$group": {"_id": "$generos", "media_nota": {"$avg": "$nota"}}},
        {"$sort": {"media_nota": -1}}
    ]
    return list(db.aggregate(pipeline))

def buscar_filmes_avancado(collection, generos, ano_min, nota_min):
    query = {
        "generos": {"$all": generos},  # Agora exige que TODOS os gêneros estejam presentes
        "ano_lancamento": {"$gte": ano_min},
        "nota": {"$gte": nota_min}
    }
    return collection.find(query)


