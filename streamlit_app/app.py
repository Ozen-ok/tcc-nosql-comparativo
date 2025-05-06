import streamlit as st


st.logo(
    "streamlit_app\assets\Logo_UEMA.png", 
    size="medium", 
    link="https://www.uema.br/", 
    icon_image="streamlit_app\assets\Logo_UEMA.png"
)

# Definindo as páginas (arquivos Python para cada banco de dados)
mongodb_page = st.Page("pages/1_MongoDB.py", title="MongoDB Operations", icon=":material/add_circle:")
cassandra_page = st.Page("pages/2_Cassandra.py", title="Cassandra Operations", icon=":material/storage:")
neo4j_page = st.Page("pages/3_Neo4j.py", title="Neo4j Operations", icon=":material/layers:")
redis_page = st.Page("pages/4_Redis.py", title="Redis Operations", icon=":material/cloud:")

# Definindo a navegação com as páginas
pg = st.navigation([mongodb_page, cassandra_page, neo4j_page, redis_page])

# Configurações da página
st.set_page_config(page_title="Comparador de Bancos NoSQL", page_icon=":material/edit:")

# Iniciando a navegação
pg.run()
