import streamlit as st
import pandas as pd
import ast
from adaptadores.mongo_api import (
    inserir_filme,
    buscar_filmes_por_genero,
    atualizar_nota,
    deletar_filme,
    contar_filmes_por_ano,
    media_por_genero,
    busca_avancada
)

def render():
    st.title("🟢 MongoDB - Operações")

    operacao = st.selectbox(
        "Escolha uma operação",
        ["Inserção", "Consulta", "Atualização", "Remoção", "Agregação: Média por Gênero", "Agregação: Contagem por Ano", "Índices (Busca Avançada)"]
    )

    if operacao == "Inserção":
        st.subheader("📥 Inserir Dados")
        with st.form("inserir_form"):
            titulo_id = st.text_input("ID do Título", "tt0000001")
            titulo = st.text_input("Título", "Exemplo")
            ano_lancamento = st.number_input("Ano de Lançamento", 1900, 2100, 2020)
            generos = st.text_input("Gêneros (separados por vírgula)", "Drama,Comédia")
            nota = st.number_input("Nota", 0.0, 10.0, 7.5)
            submitted = st.form_submit_button("Inserir")

        if submitted:
            resposta = inserir_filme(titulo_id, titulo, ano_lancamento, generos, nota)
            if "error" in resposta:
                st.error(f"Erro: {resposta['error']}")
            else:
                st.success("Filme inserido com sucesso.")
                st.json(resposta)

    elif operacao == "Consulta":
        st.subheader("🔍 Buscar Filmes por Gênero")
        genero = st.text_input("Gêneros (separados por vírgula)", "Drama,Comédia")
        if st.button("Buscar"):
            filmes = buscar_filmes_por_genero(genero)
            if "error" in filmes:
                st.error(f"Erro: {filmes['error']}")
            else:
                st.json(filmes)

    elif operacao == "Atualização":
        st.subheader("✏️ Atualizar Nota de um Filme")
        titulo_id_update = st.text_input("Título ID", "tt0000001")
        nova_nota = st.number_input("Nova Nota", 0.0, 10.0, 8.0)
        if st.button("Atualizar Nota"):
            resposta = atualizar_nota(titulo_id_update, nova_nota)
            if "error" in resposta:
                st.error(f"Erro: {resposta['error']}")
            else:
                st.success("Nota atualizada com sucesso.")
                st.json(resposta)

    elif operacao == "Remoção":
        st.subheader("🗑️ Deletar Filme")
        titulo_id_delete = st.text_input("Título ID", "tt0000001")
        if st.button("Deletar"):
            resposta = deletar_filme(titulo_id_delete)
            if "error" in resposta:
                st.error(f"Erro: {resposta['error']}")
            else:
                st.success("Filme deletado.")
                st.json(resposta)

    elif operacao == "Agregação: Média por Gênero":
        st.subheader("📊 Média de Notas por Gênero")
        if st.button("Calcular Média"):
            resposta = media_por_genero()
            if "error" in resposta:
                st.error(f"Erro: {resposta['error']}")
            else:
                st.json(resposta)

    elif operacao == "Agregação: Contagem por Ano":
        st.subheader("📈 Contar Filmes por Ano")
        if st.button("Contar"):
            resposta = contar_filmes_por_ano()
            if "error" in resposta:
                st.error(f"Erro: {resposta['error']}")
            else:
                st.json(resposta)

    elif operacao == "Índices (Busca Avançada)":
        st.subheader("🔎 Busca Avançada por Filtros")
        genero_b = st.text_input("Gêneros", "Drama")
        ano_min = st.number_input("Ano Mínimo", 1900, 2100, 2000)
        nota_min = st.number_input("Nota Mínima", 0.0, 10.0, 7.0)

        if st.button("Buscar Avançado"):
            filmes = busca_avancada(genero_b, ano_min, nota_min)
            if "error" in filmes:
                st.error(f"Erro: {filmes['error']}")
            elif "warning" in filmes:
                st.warning(filmes["warning"])
            else:
                df = pd.DataFrame(filmes)
                for _, row in df.iterrows():
                    col1, col2 = st.columns([1, 4])
                    with col1:
                        st.image(row.get("poster_url", ""), width=120)
                    with col2:
                        st.subheader(f"{row['titulo']} ({int(row['ano_lancamento'])})")

                        if row.get("nota", 0) == 0:
                            st.markdown("⭐ Ainda não lançado | 🗳️ Votos indisponíveis")
                        else:
                            st.markdown(f"⭐ {row['nota']} | 🗳️ {row['numero_votos']} votos")

                        generos = row.get("generos", "")
                        try:
                            generos = ', '.join(ast.literal_eval(generos)) if isinstance(generos, str) else generos
                        except:
                            pass

                        st.markdown(f"🎞️ {generos}")
                        duracao = row.get("duracao", "N/A")
                        try:
                            duracao = f"{int(duracao)} minutos"
                        except:
                            duracao = "N/A"
                        st.markdown(f"⏱️ {duracao}")

                        sinopse = row.get("sinopse", "")
                        sinopse_curta = sinopse[:200].rsplit(' ', 1)[0] + "..." if len(sinopse) > 200 else sinopse
                        st.markdown(f"🧾 {sinopse_curta}")
