import streamlit as st
from components.operacoes import inserir, buscar_1, atualizar, deletar, media, contar, buscar_2

if st.button("Home"):
        st.switch_page("pages/Home.py")

st.title("Comparador de Bancos NoSQL")

st.write("Bem-vindo ao nosso aplicativo de comparação de bancos de dados NoSQL!")
st.write("Aproveite a experiência!")
#st.image("assets/plankton.png", caption="Plankton - O melhor amigo do desenvolvedor!")
st.write("Desenvolvido por Ozen e equipe.")

st.subheader("🟢 MongoDB, Cassandra, Neo4j e Redis - Operações")

operacao = st.selectbox(
    "Escolha uma operação",
    ["Inserção", "Consulta", "Atualização", "Remoção", "Agregação: Média por Gênero", "Agregação: Contagem por Ano", "Índices (Busca Avançada)"]
)

if operacao == "Inserção":
    inserir()

elif operacao == "Consulta":
    buscar_1()

elif operacao == "Atualização":
    atualizar()   

elif operacao == "Remoção":
    deletar()

elif operacao == "Agregação: Média por Gênero":
    media()

elif operacao == "Agregação: Contagem por Ano":
    contar()

elif operacao == "Índices (Busca Avançada)":
    buscar_2()
