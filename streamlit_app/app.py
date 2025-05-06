import streamlit as st

st.logo(
    "https://raw.githubusercontent.com/Ozen-ok/imdb/refs/heads/main/streamlit_app/assets/Logo_UEMA.png", 
    size="large", 
    link="https://www.uema.br/", 
    icon_image="https://raw.githubusercontent.com/Ozen-ok/imdb/refs/heads/main/streamlit_app/assets/Logo_UEMA.png"
)

pages = {
    "Inicio": [
        st.Page("pages/Home.py", title="Home", icon=":material/house:"),
    ],

    "Bancos": [
        st.Page("pages/mongodb_page.py", title="MongoDB Operations", icon=":material/add_circle:"),
        st.Page("pages/cassandra_page.py", title="Cassandra Operations", icon=":material/storage:"),
        st.Page("pages/neo4j_page.py", title="Neo4j Operations", icon=":material/layers:"),
        st.Page("pages/redis_page.py", title="Redis Operations", icon=":material/cloud:")
    ],
}

pg = st.navigation(pages)
pg.run()
