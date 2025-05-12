import streamlit as st
import pandas as pd
from components.repetidor import preparar_dados_filmes
from adaptadores.api_client import (
    inserir_filme, buscar_filmes_por_genero, atualizar_nota, deletar_filme,
    media_por_genero, contar_filmes_por_ano, busca_avancada, executar_em_todos_bancos
)
from PIL import Image
import plotly.express as px
import os

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

# ==================== HANDLERS DAS FUNCIONALIDADES ====================

def handle_inserir(titulo_id, titulo, tipo, ano_lancamento, generos, nota, numero_votos, duracao, sinopse):
    return executar_em_todos_bancos(inserir_filme, titulo_id, titulo, tipo, ano_lancamento, generos, nota, numero_votos, duracao, sinopse)

def handle_buscar_por_genero(genero):
    return executar_em_todos_bancos(buscar_filmes_por_genero, genero)

def handle_atualizar_nota(titulo_id, nova_nota):
    return executar_em_todos_bancos(atualizar_nota, titulo_id, nova_nota)

def handle_deletar(titulo_id):
    return executar_em_todos_bancos(deletar_filme, titulo_id)

def handle_media_por_genero():
    return executar_em_todos_bancos(media_por_genero)

def handle_contar_por_ano():
    return executar_em_todos_bancos(contar_filmes_por_ano)

def handle_busca_avancada(genero, ano_min, nota_min):
    return executar_em_todos_bancos(busca_avancada, genero, ano_min, nota_min)

# ==================== TELAS INTERATIVAS ====================

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

def buscar_1():
    st.subheader("🔍 Buscar Filmes por Gênero")
    genero = st.text_input("Gêneros (separados por vírgula)", "Drama,Comédia")
    if st.button("Buscar"):
        resultados = handle_buscar_por_genero(genero)
        for banco, filmes_raw in resultados.items():
            if "error" in filmes_raw:
                st.error(f"{banco}: {filmes_raw['error']}")
            elif not filmes_raw:
                st.warning(f"{banco}: Nenhum resultado encontrado.")
            else:
                print(f"\nDEBUG: {banco} - tipo: {type(filmes)} - qtd: {len(filmes)}\n")
                filmes = filmes_raw.get("dados", {}).get("filmes", [])
                for row in preparar_dados_filmes(filmes):
                    exibir_cartao_filme(row)


def atualizar():
    st.subheader("✏️ Atualizar Nota de um Filme")
    titulo_id_update = st.text_input("Título ID", "tt0000001")
    nova_nota = st.number_input("Nova Nota", 0.0, 10.0, 8.0)
    if st.button("Atualizar Nota"):
        resultados = handle_atualizar_nota(titulo_id_update, nova_nota)
        for banco, resposta in resultados.items():
            st.error(f"{banco}: {resposta['error']}") if "error" in resposta else st.success(f"{banco}: Nota atualizada com sucesso.")

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
                    # Criar um novo DataFrame a partir dos dicionários dentro da coluna 'media_notas_por_genero'
                    df = pd.DataFrame(resposta)
                    
                    # Converter a coluna que contém os dicionários para duas colunas separadas
                    df = pd.json_normalize(df["media_notas_por_genero"])
                    
                    # Verificando se as colunas corretas estão presentes
                    if 'genero' in df.columns and 'media_nota' in df.columns:
                        # Geração do gráfico
                        fig = px.bar(df, x='genero', y='media_nota', 
                                     title=f"Média de Notas por Gênero - {banco}", 
                                     labels={"genero": "Gênero", "media_nota": "Média"}, 
                                     color='genero')
                        st.plotly_chart(fig)
                    else:
                        st.error(f"O DataFrame para o banco {banco} não contém as colunas esperadas 'genero' e 'media_nota'.")
                except Exception as e:
                    st.error(f"Erro ao processar os dados para o banco {banco}. Detalhes: {str(e)}")

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
                    # Certifica-se de que a resposta seja uma lista de dicionários
                    dados = [item for item in resposta]  # Garante que é uma lista de dicionários
                    df = pd.DataFrame(dados)

                    # Exibe o DataFrame (opcional, pode ser removido se não necessário)
                    st.write(df)

                    # Geração do gráfico
                    fig = px.bar(df, x='_id', y='quantidade', 
                                 title=f"Contagem de Filmes por Ano - {banco}", 
                                 labels={"_id": "Ano", "quantidade": "Quantidade"}, 
                                 color='_id')
                    st.plotly_chart(fig)
                except Exception as e:
                    st.error(f"Erro ao processar os dados para o banco {banco}. Detalhes: {str(e)}")

def buscar_2():
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
                for row in preparar_dados_filmes(filmes_raw):
                    exibir_cartao_filme(row)
