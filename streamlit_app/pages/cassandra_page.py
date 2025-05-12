import streamlit as st
import pandas as pd
from adaptadores.cassandra_api_client import (
    inserir_filme,
    buscar_filmes_por_genero,
    atualizar_nota,
    deletar_filme,
    contar_filmes_por_ano,
    media_por_genero,
    busca_avancada
)
from components.repetidor import criar_botao_home, preparar_dados_filmes, exibir_cartao_filme
import plotly.express as px

criar_botao_home()

st.title("🔵 Cassandra - Operações")

operacao = st.selectbox(
    "Escolha uma operação",
    ["Inserção", "Consulta", "Atualização", "Remoção", "Agregação: Média por Gênero", "Agregação: Contagem por Ano", "Índices (Busca Avançada)"]
)

if operacao == "Inserção":
    st.subheader("📥 Inserir Dados")
    with st.form("inserir_form"):
        titulo_id = st.text_input("ID do Título", "tt0000001")
        titulo = st.text_input("Título", "Exemplo")
        tipo = st.selectbox("Escolha um tipo", ["Filme", "Série de TV", "Filme para TV", "Vídeo", "Jogo", "Minissérie", "Curta"])
        ano_lancamento = st.number_input("Ano de Lançamento", 1900, 2025, 2020)
        generos = st.text_input("Gêneros (separados por vírgula)", "Drama,Comédia")
        nota = st.number_input("Nota", 0.0, 10.0, 7.5)
        numero_votos = st.number_input("Número de Votos", min_value=0, step=1, value=1000)
        duracao = st.number_input("Duração (em minutos)", min_value=0, step=1, value=120)
        sinopse = st.text_area("Sinopse", "Descrição do filme...")

        submitted = st.form_submit_button("Inserir")

    if submitted:
        resposta = inserir_filme(
            titulo_id, titulo, tipo, ano_lancamento, generos, nota,
            numero_votos, duracao, sinopse
        )
        if "error" in resposta:
            st.error(f"{resposta['error']}")
        else:
            st.success("Filme inserido com sucesso.")

elif operacao == "Consulta":
    st.subheader("🔍 Buscar Filmes por Gênero")
    genero = st.text_input("Gêneros (separados por vírgula)", "Drama,Comédia")
    if st.button("Buscar"):
        filmes_raw = buscar_filmes_por_genero(genero)
        if "error" in filmes_raw:
            st.error(f"{filmes_raw['error']}")
        elif not filmes_raw:
            st.warning("Nenhum resultado encontrado.")
        else:
            filmes = preparar_dados_filmes(filmes_raw)
            for row in filmes:
                exibir_cartao_filme(row)


elif operacao == "Atualização":
    st.subheader("✏️ Atualizar Nota de um Filme")
    titulo_id_update = st.text_input("Título ID", "tt0000001")
    nova_nota = st.number_input("Nova Nota", 0.0, 10.0, 8.0)
    if st.button("Atualizar Nota"):
        resposta = atualizar_nota(titulo_id_update, nova_nota)
        if "error" in resposta:
            st.error(f"{resposta['error']}")
        else:
            st.success("Nota atualizada com sucesso.")

elif operacao == "Remoção":
    st.subheader("🗑️ Deletar Filme")
    titulo_id_delete = st.text_input("Título ID", "tt0000001")
    if st.button("Deletar"):
        resposta = deletar_filme(titulo_id_delete)
        if "error" in resposta:
            st.error(f"{resposta['error']}")
        else:
            st.success("Filme deletado.")

elif operacao == "Agregação: Média por Gênero":
    st.subheader("📊 Média de Notas por Gênero")
    if st.button("Calcular Média"):
        resposta = media_por_genero()  # Função que retorna a lista de médias
        if "error" in resposta:
            st.error(f"{resposta['error']}")
        else:
            # Converte a resposta para um DataFrame
            df = pd.DataFrame(resposta)

            # Cria um gráfico de barras com Plotly
            fig = px.bar(df, x='genero', y='media_nota',
                         title="Média de Notas por Gênero",
                         labels={"genero": "Gênero", "media_nota": "Média de Nota"},
                         color='genero')

            # Exibe o gráfico no Streamlit
            st.plotly_chart(fig)

elif operacao == "Agregação: Contagem por Ano":
    st.subheader("📈 Contar Filmes por Ano")
    if st.button("Contar"):
        resposta = contar_filmes_por_ano()  # Função que retorna a contagem de filmes por ano
        if "error" in resposta:
            st.error(f"{resposta['error']}")
        else:
            # Converte a resposta para um DataFrame
            df = pd.DataFrame(resposta)

            # Cria um gráfico de barras com Plotly
            fig = px.bar(df, x='_id', y='quantidade',
                         title="Contagem de Filmes por Ano",
                         labels={"_id": "Ano", "quantidade": "Quantidade de Filmes"},
                         color='_id')

            # Exibe o gráfico no Streamlit
            st.plotly_chart(fig)

elif operacao == "Índices (Busca Avançada)":
    st.subheader("🔎 Busca Avançada por Filtros")
    genero_b = st.text_input("Gêneros", "Drama")
    ano_min = st.number_input("Ano Mínimo", 1900, 2100, 2000)
    nota_min = st.number_input("Nota Mínima", 0.0, 10.0, 7.0)

    if st.button("Buscar Avançado"):
        filmes_raw = busca_avancada(genero_b, ano_min, nota_min)
        if "error" in filmes_raw:
            st.error(f"{filmes_raw['error']}")
        elif "warning" in filmes_raw:
            st.warning(filmes_raw["warning"])
        else:
            filmes = preparar_dados_filmes(filmes_raw)
            for row in filmes:
                exibir_cartao_filme(row)
