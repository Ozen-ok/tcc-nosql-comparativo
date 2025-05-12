import os
from dotenv import load_dotenv
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from neo4j import GraphDatabase
import redis
from pathlib import Path

# Garante que o .env seja carregado a partir da raiz do projeto
env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path)

# Carregar variáveis do .env
MONGO_HOST = os.getenv("MONGO_INITDB_ROOT_HOST")
MONGO_PORT = os.getenv("MONGO_INITDB_ROOT_PORT")
MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USER")
MONGO_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
MONGO_DB_NAME = os.getenv("MONGO_INITDB_ROOT_DB_NAME")

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT")
CASSANDRA_USER = os.getenv("CASSANDRA_USER")
CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE")

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_DB = os.getenv("REDIS_DB")


def get_mongo_db():
    uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/admin"
    client = MongoClient(uri)
    db = client[MONGO_DB_NAME]
    return db

def get_mongo_client():
    uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/admin"
    client = MongoClient(uri)
    return client

def get_cassandra_db():

    # Usando o PlainTextAuthProvider para autenticação
    auth_provider = PlainTextAuthProvider(CASSANDRA_USER, CASSANDRA_PASSWORD)

    # Criando um Cluster com as informações de conexão
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, auth_provider=auth_provider)

    # Criando uma sessão (sem especificar banco, pois no Cassandra usamos keyspaces)
    session = cluster.connect()

    # Aqui você pode especificar um keyspace, caso queira
    session.set_keyspace(CASSANDRA_KEYSPACE)

    return session

def get_neo4j_driver():
    uri = NEO4J_URI
    user = NEO4J_USER
    password = NEO4J_PASSWORD
    return GraphDatabase.driver(uri, auth=(user, password))

def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

"""""
import streamlit as st
import pandas as pd

CASSANDRA_TABLE_NAME = "dados_filtrados"
MONGO_COLLECTION_NAME = "movies"

# App
#st.title("Comparação de 'generos' entre MongoDB e Cassandra com base em 'title_id'")

try:
    # MongoDB
    mongo_db = get_mongo_db()
    mongo_collection = mongo_db[MONGO_COLLECTION_NAME]
    mongo_data = list(mongo_collection.find({}, {'_id': 0, 'titulo_id': 1, 'generos': 1}).limit(1000))
    mongo_df = pd.DataFrame(mongo_data)
    st.subheader("MongoDB - Dados carregados")
    st.dataframe(mongo_df)

    # Cassandra
    session = get_cassandra_db()
    query = f"SELECT titulo_id, generos FROM {CASSANDRA_TABLE_NAME} LIMIT 1000"
    cassandra_rows = session.execute(query)
    cassandra_df = pd.DataFrame(cassandra_rows.all())
    st.subheader("Cassandra - Dados carregados")
    st.dataframe(cassandra_df)

    # Merge e comparação
    df_comparado = pd.merge(mongo_df, cassandra_df, on="titulo_id", suffixes=("_mongo", "_cassandra"))

    df_comparado["iguais"] = df_comparado["generos_mongo"] == df_comparado["generos_cassandra"]

    st.subheader("Resultado da Comparação")
    st.dataframe(df_comparado[["titulo_id", "generos_mongo", "generos_cassandra", "iguais"]])

    # Exibir só diferenças
    diferentes = df_comparado[~df_comparado["iguais"]]
    if not diferentes.empty:
        st.subheader("Diferenças encontradas:")
        st.dataframe(diferentes[["titulo_id", "generos_mongo", "generos_cassandra"]])
    else:
        st.success("Nenhuma diferença encontrada entre os campos 'generos'!")

except Exception as e:
    st.error(f"Erro ao acessar os dados: {e}")


from pymongo.collection import Collection

def comparar_generos_mongo_cassandra(mongo_collection: Collection, cassandra_session):
    # MongoDB
    total_docs_mongo = mongo_collection.count_documents({})
    generos_mongo = mongo_collection.distinct("generos")
    print("Total de documentos (MongoDB):", total_docs_mongo)
    print("Gêneros únicos (MongoDB):", sorted(generos_mongo))

    # Cassandra
    query = "SELECT generos FROM dados_filtrados ALLOW FILTERING"
    rows = list(cassandra_session.execute(query))  # Converte para lista para usar len()
    todos_generos = []

    for row in rows:
        todos_generos.extend(row.generos)

    print("Total de registros (Cassandra):", len(rows))
    print("Gêneros únicos (Cassandra):", sorted(set(todos_generos)))

mongo_db = get_mongo_db()
mongo_collection = mongo_db[MONGO_COLLECTION_NAME]
session = get_cassandra_db()

#comparar_generos_mongo_cassandra(mongo_collection, session)

from collections import Counter

# MongoDB
titulo_ids_mongo = mongo_collection.distinct("titulo_id")
duplicados_mongo = [item for item, count in Counter(titulo_ids_mongo).items() if count > 1]
print(f"Títulos duplicados no MongoDB: {duplicados_mongo}")

# Cassandra
query = "SELECT titulo_id FROM dados_filtrados ALLOW FILTERING"
rows = list(session.execute(query))  # Obtém todos os registros do Cassandra
titulo_ids_cassandra = [row.titulo_id for row in rows]
duplicados_cassandra = [item for item, count in Counter(titulo_ids_cassandra).items() if count > 1]
print(f"Títulos duplicados no Cassandra: {duplicados_cassandra}")

"""""

