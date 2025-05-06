import streamlit as st
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd

# Conectando ao Cassandra
CASSANDRA_HOST="localhost"
CASSANDRA_PORT=9042
CASSANDRA_USER="cassandra"
CASSANDRA_PASSWORD="cassandra"
CASSANDRA_KEYSPACE="imdb_keyspace"

# Estabelecer conexão com o Cassandra
auth_provider = PlainTextAuthProvider(CASSANDRA_USER, CASSANDRA_PASSWORD)
cluster = Cluster([CASSANDRA_HOST], auth_provider=auth_provider)
session = cluster.connect()

# Seleciona o keyspace para uso
session.set_keyspace(CASSANDRA_KEYSPACE)

# Consulta com LIMIT para limitar os resultados
query = "SELECT * FROM filmes LIMIT 30"
rows = session.execute(query)

# Exibir título no Streamlit
st.title("Filmes no Cassandra")

# Organizar os dados em uma lista de dicionários
dados = []
for row in rows:
    dados.append({
        "titulo_id": row.titulo_id,
        "ator_id": row.ator_id,
        "nome_personagem": row.nome_personagem,
        "tipo": row.tipo,
        "titulo": row.titulo,
        "ano_lancamento": row.ano_lancamento,
        "generos": row.generos,
        "nome_ator": row.nome_ator,
        "ano_nascimento": row.ano_nascimento,
        "nota": row.nota,
        "numero_votos": row.numero_votos,
        "duracao": row.duracao,
        "sinopse": row.sinopse
    })

# Converter para DataFrame do Pandas
df = pd.DataFrame(dados)

# Exibir a tabela no Streamlit
st.dataframe(df)