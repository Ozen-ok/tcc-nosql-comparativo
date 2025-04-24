import streamlit as st
import pandas as pd
import ast

st.set_page_config(layout="wide")

df = pd.read_csv(r"C:\Users\ozen\Desktop\projeto_imdb\dados_filtrados.tsv", sep="\t")

# URLs das imagens
base_url_posters = "https://raw.githubusercontent.com/Ozen-ok/imdb/main/posters/"
df["poster_url"] = df["titulo_id"].apply(lambda tid: base_url_posters + f"{tid}.jpg")

base_url_atores = "https://raw.githubusercontent.com/Ozen-ok/imdb/main/actors/"
df["actor_url"] = df["ator_id"].apply(lambda aid: base_url_atores + f"{aid}.jpg")

if "pagina" not in st.session_state:
    st.session_state.pagina = "home"

if "titulo_detalhado" not in st.session_state:
    st.session_state.titulo_detalhado = None

def ir_para_home():
    st.session_state.pagina = "home"
    st.session_state.titulo_detalhado = None

def ir_para_detalhe(titulo):
    st.session_state.pagina = "detalhe"
    st.session_state.titulo_detalhado = titulo

# Página de detalhe
if st.session_state.pagina == "detalhe":
    titulo = st.session_state.titulo_detalhado
    st.button("⬅️ Voltar", on_click=ir_para_home)

    st.header(f"{titulo['titulo']} ({int(titulo['ano_lancamento'])})")
    st.image(titulo["poster_url"], use_container_width=False)

    if titulo["nota"] == 0:
        st.markdown("⭐ **Nota IMDb:** Ainda não lançado")
        st.markdown("🗳️ **Número de votos:** Ainda não disponível")
    else:
        st.markdown(f"⭐ **Nota IMDb:** {titulo['nota']}")
        st.markdown(f"🗳️ **Número de votos:** {titulo['numero_votos']}")

    st.markdown(f"🎭 **Ator:** {titulo['nome_ator']}")
    st.markdown(f"**Sinopse:** {titulo['sinopse']}")

    try:
        generos_formatados = ', '.join(ast.literal_eval(titulo['generos']))
    except:
        generos_formatados = titulo['generos']
    st.markdown(f"🎞️ **Gêneros:** {generos_formatados}")

    st.markdown(f"📺 **Tipo:** {titulo['tipo']}")

    try:
        duracao = f"{int(titulo['duracao'])} minutos"
    except:
        duracao = "N/A"
    st.markdown(f"⏱️ **Duração:** {duracao}")

# Página inicial
else:
    st.title("🎬 Catálogo de Títulos por Ator")

    col1, col2 = st.columns([1, 5])
    with col1:
        atores_unicos = df[["ator_id", "nome_ator"]].drop_duplicates().reset_index(drop=True)
        ator_escolhido = st.selectbox("Selecione um ator:", atores_unicos["nome_ator"])

    ator_id = atores_unicos[atores_unicos["nome_ator"] == ator_escolhido]["ator_id"].values[0]
    resultados = df[df["ator_id"] == ator_id].sort_values(by="ano_lancamento", ascending=False)
    resultados = resultados.drop_duplicates(subset="titulo_id")

    st.markdown(f"### Resultados para **{ator_escolhido}**:")

    for idx, row in resultados.iterrows():
        col1, col2 = st.columns([1, 4])
        with col1:
            st.image(row["poster_url"], width=120)
        with col2:
            st.subheader(f"{row['titulo']} ({int(row['ano_lancamento'])})")

            if row["nota"] == 0:
                st.markdown("⭐ Ainda não lançado | 🗳️ Votos indisponíveis")
            else:
                st.markdown(f"⭐ {row['nota']} | 🗳️ {row['numero_votos']} votos")

            try:
                duracao = f"{int(row['duracao'])} minutos"
            except:
                duracao = "N/A"

            try:
                generos = ', '.join(ast.literal_eval(row["generos"]))
            except:
                generos = row["generos"]

            st.markdown(f"🎞️ {generos}")
            st.markdown(f"⏱️ {duracao}")
            sinopse_curta = row['sinopse'][:200].rsplit(' ', 1)[0] + "..."
            st.markdown(f"🧾 {sinopse_curta}")

            st.button(
                "🔍 Ver detalhes",
                key=f"{row['titulo_id']}_{idx}",
                on_click=ir_para_detalhe,
                args=(row,)
            )
