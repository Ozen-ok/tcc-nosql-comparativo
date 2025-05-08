import streamlit as st
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd

CASSANDRA_TABLE_NAME = "dados_filtrados"
MONGO_COLLECTION_NAME = "movies"
def get_mongo_db():
    uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/admin"
    client = MongoClient(uri)
    db = client[MONGO_DB_NAME]
    return db

def get_cassandra_db():
    auth_provider = PlainTextAuthProvider(CASSANDRA_USER, CASSANDRA_PASSWORD)
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace(CASSANDRA_KEYSPACE)
    return session

# Streamlit app
st.title("Comparação da Coluna 'generos' entre MongoDB e Cassandra")

# Tentando carregar os dados
try:
    # MongoDB
    mongo_db = get_mongo_db()
    mongo_collection = mongo_db[MONGO_COLLECTION_NAME]
    mongo_data = list(mongo_collection.find({}, {'_id': 0, 'generos': 1}).limit(100))
    mongo_df = pd.DataFrame(mongo_data)
    st.subheader("MongoDB - Coluna 'generos'")
    st.dataframe(mongo_df)

    # Cassandra
    session = get_cassandra_db()
    query = f"SELECT generos FROM {CASSANDRA_TABLE_NAME} LIMIT 100"
    rows = session.execute(query)
    cassandra_df = pd.DataFrame(rows.all())
    st.subheader("Cassandra - Coluna 'generos'")
    st.dataframe(cassandra_df)

    # Comparação
    if len(mongo_df) != len(cassandra_df):
        st.warning("Os DataFrames têm tamanhos diferentes. Comparação linha a linha pode estar desalinhada.")
    
    comparacao = mongo_df['generos'] == cassandra_df['generos']
    st.subheader("Resultado da Comparação linha a linha (True = iguais)")
    st.dataframe(comparacao)

    # Mostrar diferenças
    diferencas = pd.DataFrame({
        "MongoDB": mongo_df['generos'],
        "Cassandra": cassandra_df['generos'],
        "Iguais?": comparacao
    })
    diferencas_filtradas = diferencas[~comparacao]
    if not diferencas_filtradas.empty:
        st.subheader("Diferenças encontradas na coluna 'generos':")
        st.dataframe(diferencas_filtradas)
    else:
        st.success("Nenhuma diferença encontrada na coluna 'generos'!")

except Exception as e:
    st.error(f"Erro ao acessar os dados: {e}")
