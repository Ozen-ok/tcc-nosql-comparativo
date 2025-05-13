import streamlit as st
from components.operacoes import inserir, realizar_busca_simples, atualizar, deletar, media, contar, busca_ava, listar_filmes_por_ator, atores_por_filme
if st.button("Home"):
        st.switch_page("pages/Home.py")

st.title("Comparador de Bancos NoSQL")

st.write("Bem-vindo ao nosso aplicativo de comparação de bancos de dados NoSQL!")
st.write("Aproveite a experiência!")
#st.image("assets/plankton.png", caption="Plankton - O melhor amigo do desenvolvedor!")
st.write("Desenvolvido por Ozen e equipe.")


st.title("🟢 MongoDB, Cassandra, Neo4j e Redis - Operações")

operacao = st.selectbox(
    "Escolha uma operação",
    [
        "Inserir Filme",
        "Buscar Filmes por Campo (Simples)",
        "Atualizar Campo do Filme",
        "Remover Filme",
        "Agregação: Média por Gênero",
        "Agregação: Contagem por Ano",
        "Índices (Busca Avançada)",
        "Listar Atores por Filme",
        "Listar Filmes por Ator"
    ]
)

if operacao == "Inserir Filme":
    inserir()

elif operacao == "Buscar Filmes por Campo (Simples)":
    realizar_busca_simples()

elif operacao == "Atualizar Campo do Filme":
    atualizar()

elif operacao == "Remover Filme":
    deletar()

elif operacao == "Agregação: Média por Gênero":
    media()

elif operacao == "Agregação: Contagem por Ano":
    contar()

elif operacao == "Índices (Busca Avançada)":
    busca_ava()

elif operacao == "Listar Filmes por Ator":
    listar_filmes_por_ator()

elif operacao == "Listar Atores por Filme":
    atores_por_filme()

