import streamlit as st
import pandas as pd
from components.repetidor import preparar_dados_filmes, preparar_dados_atores_por_filme
from adaptadores.api_client import (
    inserir_filme, deletar_filme, busca_simples, atualizar_campo, filmes_por_ator,
    media_por_genero, contar_filmes_por_ano, busca_avancada, listar_atores_por_filme, executar_em_todos_bancos
)
from PIL import Image
import plotly.express as px
import os


# ==================== HANDLERS DAS FUNCIONALIDADES ====================

def handle_inserir(titulo_id, titulo, tipo, ano_lancamento, generos, nota, numero_votos, duracao, sinopse):
    return executar_em_todos_bancos(inserir_filme, titulo_id, titulo, tipo, ano_lancamento, generos, nota, numero_votos, duracao, sinopse)

def handle_busca_simples(campo, valor):
    return executar_em_todos_bancos(busca_simples, campo, valor)

def handle_atualizar_campo(titulo_id, campo, novo_valor):
    return executar_em_todos_bancos(atualizar_campo, titulo_id, campo, novo_valor)

def handle_deletar(titulo_id):
    return executar_em_todos_bancos(deletar_filme, titulo_id)

def handle_media_por_genero():
    return executar_em_todos_bancos(media_por_genero)

def handle_contar_por_ano():
    return executar_em_todos_bancos(contar_filmes_por_ano)

def handle_busca_avancada(genero, ano_min, nota_min):
    return executar_em_todos_bancos(busca_avancada, genero, ano_min, nota_min)

def handle_listar_atores_por_filme(titulo_id):
    return executar_em_todos_bancos(listar_atores_por_filme, titulo_id)

def handle_filmes_por_ator(nome_ator):
    return executar_em_todos_bancos(filmes_por_ator, nome_ator)

# ==================== TELAS INTERATIVAS ====================

generos_lista = ["Ação", "Aventura", "Animação", "Biografia", 
        "Comédia", "Crime", "Documentário", "Drama", 
        "Família", "Fantasia", "História", "Terror", 
        "Música", "Musical", "Mistério", "Romance",
        "Ficção Científica", "Esporte", "Suspense", 
        "Guerra", "Faroeste"]

atores_lista = [
    "Norman Reedus", "Steven Yeun", "Keanu Reeves", "Ella Purnell",
    "Hailee Steinfeld", "Jeffrey Dean Morgan", "Austin Butler", "Timothée Chalamet",
    "Elle Fanning", "Troy Baker", "Jenna Ortega", "Emma Myers", 
    "Sebastian Stan", "J.K. Simmons", "Kiernan Shipka", "Ana de Armas", 
    "Mikey Madison", "Mia Goth"
]

# ==================== COMPONENTE DE EXIBIÇÃO DE CARTÃO DE FILME ====================

def exibir_cartao_filme(row):
    col1, col2 = st.columns([1, 4])
    with col1:
        poster_path = row.get("poster_url", "")
        if os.path.exists(poster_path):
            imagem = Image.open(poster_path)
            st.image(imagem)
        else:
            st.warning(f"Imagem não encontrada para {row['titulo']}")
    with col2:
        st.subheader(f"{row['titulo']} ({int(row['ano_lancamento'])})")
        nota = row.get("nota", 0)
        if nota == 0:
            st.markdown("⭐ Ainda não lançado | 🗳️ Votos indisponíveis")
        else:
            st.markdown(f"⭐ {round(nota, 2)} | 🗳️ {row['numero_votos']} votos")
        st.markdown(f"🎬 Tipo: {row.get('tipo', 'Desconhecido')}")

        generos_raw = row.get("generos", "")
        generos_lista = eval(generos_raw) if isinstance(generos_raw, str) and generos_raw.startswith("[") else (
            generos_raw if isinstance(generos_raw, list) else [generos_raw]
        )
        st.markdown(f"🎞️ {', '.join(str(g).strip('\'\"') for g in generos_lista)}")

        duracao = row.get("duracao", "N/A")
        try:
            duracao = f"{int(float(duracao))} minutos"
        except:
            duracao = "N/A"

        if row.get("tipo", "").lower() != "jogo":
            st.markdown(f"⏱️ {duracao}")

        sinopse = row.get("sinopse", "")
        sinopse_curta = sinopse[:200].rsplit(' ', 1)[0] + "..." if isinstance(sinopse, str) and len(sinopse) > 200 else (
            sinopse if isinstance(sinopse, str) else "Sinopse indisponível.")
        st.markdown(f"🧾 {sinopse_curta}")

def inserir():
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
        resultados = handle_inserir(titulo_id, titulo, tipo, ano_lancamento, generos, nota, numero_votos, duracao, sinopse)
        for banco, resposta in resultados.items():
            st.error(f"{banco}: {resposta['error']}") if "error" in resposta else st.success(f"{banco}: Filme inserido com sucesso.")

def realizar_busca_simples():
    st.subheader("🔍 Busca Simples por Campo")
    
     # Campos possíveis
    campos = ["titulo", "tipo", "ano_lancamento", "generos", "nota"]
    campo = st.selectbox("Escolha o campo para buscar", campos)

    # Dependendo do campo escolhido, mostra o input adequado
    if campo == "titulo":
        valor = st.text_input("Valor a buscar (Título)", "")
    elif campo == "nota":
        valor = st.number_input("Nota (0 a 10)", 0.0, 10.0, 7.0, step=0.1)
    elif campo == "ano_lancamento":
        valor = st.number_input("Ano de Lançamento (1900 a 2029)", 1900, 2029, 2000)
    elif campo == "generos":
        selecionados = st.multiselect("Escolha os Gêneros", generos_lista)
        valor = ",".join(selecionados)  # Transforma a lista em uma string separada por vírgula
    elif campo == "tipo":
        tipos_disponiveis = ["Filme", "Série de TV", "Filme para TV", "Vídeo", "Jogo", "Minissérie", "Curta"]
        valor = st.selectbox("Escolha o Tipo", tipos_disponiveis)

    if st.button("Buscar"):
        resultados = handle_busca_simples(campo, valor)

        for banco, filmes_raw in resultados.items():
            if "error" in filmes_raw:
                st.error(f"{banco}: {filmes_raw['error']}")
            elif not filmes_raw:
                st.warning(f"{banco}: Nenhum resultado encontrado.")
            else:
                filmes = filmes_raw.get("filmes", [])
                for row in preparar_dados_filmes(filmes):
                    exibir_cartao_filme(row)

def atualizar():
    st.subheader("✏️ Atualizar Campo um Filme")
    titulo_id_update = st.text_input("Título ID", "tt0000001")
     # Campos possíveis
    campos = ["titulo", "tipo", "ano_lancamento", "generos", "nota", "numero_votos", "duracao", "sinopse"]
    campo = st.selectbox("Escolha o campo para buscar", campos)

    # Dependendo do campo escolhido, mostra o input adequado
    if campo == "titulo":
        valor = st.text_input("Título", "Exemplo")
    elif campo == "tipo":
        tipos_disponiveis = ["Filme", "Série de TV", "Filme para TV", "Vídeo", "Jogo", "Minissérie", "Curta"]
        valor = st.selectbox("Escolha o Tipo", tipos_disponiveis)
    elif campo == "ano_lancamento":
        valor = st.number_input("Ano de Lançamento", 1900, 2025, 2020)
    elif campo == "generos":
        selecionados = st.multiselect("Escolha os Gêneros", generos_lista)
        valor = ",".join(selecionados)  # Transforma a lista em uma string separada por vírgula
    elif campo == "nota":
        valor = st.number_input("Nota (0 a 10)", 0.0, 10.0, 7.0, step=0.1)
    elif campo == "numero_votos":
        valor = st.number_input("Número de Votos", min_value=1, step=1, value=1000)
    elif campo == "duracao":
        valor = st.number_input("Duração (em minutos)", min_value=0, step=1, value=120)
    elif campo == "sinopse":
        valor = st.text_area("Sinopse", "Descrição do filme...")

    if st.button("Atualizar Campo"):
        resultados = handle_atualizar_campo(titulo_id_update, campo, valor)
        for banco, resposta in resultados.items():
            st.error(f"{banco}: {resposta['error']}") if "error" in resposta else st.success(f"{banco}: Campo atualizado com sucesso.")

def deletar():
    st.subheader("🗑️ Deletar Filme")
    titulo_id_delete = st.text_input("Título ID", "tt0000001")
    if st.button("Deletar"):
        resultados = handle_deletar(titulo_id_delete)
        for banco, resposta in resultados.items():
            st.error(f"{banco}: {resposta['error']}") if "error" in resposta else st.success(f"{banco}: Filme deletado.")

def media():
    st.subheader("📊 Média de Notas por Gênero")
    
    if st.button("Calcular Média"):
        resultados = handle_media_por_genero()
        
        for banco, resposta in resultados.items():
            if "error" in resposta:
                st.error(f"{banco}: {resposta['error']}")
            else:
                try:
                    df = pd.DataFrame(resposta.get("media_notas_por_genero", []))
                    if 'genero' in df.columns and 'media_nota' in df.columns:
                        fig = px.bar(df, x='genero', y='media_nota',
                                     title=f"Média de Notas por Gênero - {banco}",
                                     labels={"genero": "Gênero", "media_nota": "Média"},
                                     color='genero')
                        st.plotly_chart(fig)
                    else:
                        st.error(f"{banco}: Dados incompletos.")
                except Exception as e:
                    st.error(f"{banco}: Erro ao processar dados. Detalhes: {e}")


def contar():
    st.subheader("📈 Contar Filmes por Ano")
    
    if st.button("Contar"):
        # Chama a função que lida com a contagem por ano
        resultados = handle_contar_por_ano()
        
        for banco, resposta in resultados.items():
            if "error" in resposta:
                st.error(f"{banco}: {resposta['error']}")
            else:
                try:
                    df = pd.DataFrame(resposta.get("contagem_por_ano", []))

                    # Geração do gráfico
                    fig = px.bar(df, x='_id', y='quantidade', 
                                 title=f"Contagem de Filmes por Ano - {banco}", 
                                 labels={"_id": "Ano", "quantidade": "Quantidade"}, 
                                 color='_id')
                    st.plotly_chart(fig)
                except Exception as e:
                    st.error(f"Erro ao processar os dados para o banco {banco}. Detalhes: {str(e)}")

def busca_ava():
    st.subheader("🔎 Busca Avançada por Filtros")
    genero_b = st.text_input("Gêneros", "Drama")
    ano_min = st.number_input("Ano Mínimo", 1900, 2100, 2000)
    nota_min = st.number_input("Nota Mínima", 0.0, 10.0, 7.0)
    
    if st.button("Buscar Avançado"):
        resultados = handle_busca_avancada(genero_b, ano_min, nota_min)
        
        for banco, filmes_raw in resultados.items():
            if "error" in filmes_raw:
                st.error(f"{banco}: {filmes_raw['error']}")
            elif "warning" in filmes_raw:
                st.warning(f"{banco}: {filmes_raw['warning']}")
            else:
                # Acessa a chave "filmes" diretamente
                filmes = filmes_raw.get("filmes", [])
                for row in preparar_dados_filmes(filmes):
                    exibir_cartao_filme(row)

def listar_filmes_por_ator():
    st.subheader("🧑‍🎤 Listar Filmes por Ator")
    nome_ator = st.selectbox("Escolha o Ator", atores_lista)

    if st.button("Listar Filmes do Ator"):
        resultados = handle_filmes_por_ator(nome_ator)

        for banco, resposta in resultados.items():
            if "error" in resposta:
                st.error(f"{banco}: {resposta['error']}")
            elif "warning" in resposta:
                st.warning(f"{banco}: {resposta['warning']}")
            else:
                filmes = resposta.get("filmes", [])
                st.markdown(f"**{banco}**:")
                #st.write(atores)
                for row in preparar_dados_filmes(filmes):
                    exibir_cartao_filme(row)

def atores_por_filme():
    st.subheader("🧑‍🎭 Atores de um Filme")

    titulo_id = st.text_input("Digite o ID do título (ex: tt1234567)", "tt24179294")
    
    if st.button("Buscar Atores"):
        resultados = handle_listar_atores_por_filme(titulo_id)

        for banco, resposta in resultados.items():
            st.markdown(f"### Banco: {banco}")

            if "error" in resposta:
                st.error(f"{banco}: {resposta['error']}")
                continue
            
            atores_brutos = resposta.get("atores", [])
            atores = preparar_dados_atores_por_filme(atores_brutos)

            if not atores:
                st.warning("Nenhum ator encontrado.")
                continue

            for ator in atores:
                st.image(ator["poster_url"], caption=f"Foto de {ator['nome_ator']}", width=150)

                with st.expander(f"👤 {ator['nome_ator']}"):
                    st.markdown(f"**Nome:** {ator['nome_ator']}")
                    st.markdown(f"**Ano de Nascimento:** {ator.get('ano_nascimento', 'N/A')}")
                    st.markdown(f"**Personagem no filme:** {ator.get('nome_personagem', 'Desconhecido')}")

                    st.markdown("##### 🎞️ Participações em outros filmes:")
                    for titulo in ator.get("titulos", []):
                        poster_path = f"assets/imagens/posters/{titulo['titulo_id']}.jpg"
                        st.image(poster_path, caption=f"{titulo['titulo']} ({titulo['ano']})", width=200)




