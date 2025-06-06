# src/streamlit_app/paginas/media.py
import streamlit as st
from components.ui_elements import seletor_de_banco_global_sidebar # Importa o seletor global
from components.display_utils import exibir_grafico_media_notas_genero
from services.operation_handlers import handle_media_notas_por_genero_operacao
from config.settings import BANCOS_SUPORTADOS

st.set_page_config(layout="wide", page_title="Média de Notas por Gênero")
st.title("📊 Média de Notas por Gênero")
st.markdown("Analise a média das notas de avaliação dos filmes, agrupadas por gênero, para o banco de dados selecionado.")

# --- Chave única para o CONTEXTO de resultado desta página ---
CHAVE_SESSAO_CONTEXTO_MEDIA = "contexto_ultima_media_genero"

# --- 1. Seleção de Banco de Dados Global ---
# A função já desenha o seletor na sidebar e retorna a chave interna ("mongo", "todos", etc.)
banco_global_atual_chave = seletor_de_banco_global_sidebar()
nome_banco_amigavel = BANCOS_SUPORTADOS.get(banco_global_atual_chave, banco_global_atual_chave.capitalize())
#st.sidebar.caption(f"Operando em: {nome_banco_amigavel}")

st.markdown(f"Gere um gráfico de média de notas para **{nome_banco_amigavel.lower()}**")

# --- 2. Botão e Lógica de Busca ---
if st.button("📊 Gerar Gráfico de Média de Notas"):
    with st.spinner(f"Calculando média de notas por gênero em '{nome_banco_amigavel}'..."):
        resposta_api = handle_media_notas_por_genero_operacao(
            banco_selecionado=banco_global_atual_chave
        )
        # Salva o CONTEXTO completo: os dados da API e o banco que foi usado para a busca,
        # sobrescrevendo o contexto antigo.
        st.session_state[CHAVE_SESSAO_CONTEXTO_MEDIA] = {
            "dados_api": resposta_api, 
            "banco_chave_usado": banco_global_atual_chave 
        }
        
# --- 3. Lógica de Exibição (sempre usa o contexto salvo) ---
# Se existe um contexto salvo no session_state, exibe o resultado dele.
if CHAVE_SESSAO_CONTEXTO_MEDIA in st.session_state:
    # Pega o contexto que foi salvo na última vez que o botão foi clicado
    contexto_salvo = st.session_state[CHAVE_SESSAO_CONTEXTO_MEDIA]
    
    # Extrai os dados e a chave do banco daquele contexto salvo
    resposta_api_salva = contexto_salvo["dados_api"]
    banco_chave_dos_dados = contexto_salvo["banco_chave_usado"]
    
    # Chama a função de display passando os dados salvos e o banco ao qual eles PERTENCEM.
    # Assim, o título do gráfico ("Média... - MongoDB") estará sempre correto,
    # mesmo que você já tenha mudado o seletor da sidebar para "Cassandra".
    exibir_grafico_media_notas_genero(
        resposta_api_completa=resposta_api_salva,
        banco_selecionado_chave=banco_chave_dos_dados 
    )

# --- 4. (Opcional) Botão para Limpar ---
st.markdown("---")
if st.button("Limpar Gráfico(s)"):
    if CHAVE_SESSAO_CONTEXTO_MEDIA in st.session_state:
        del st.session_state[CHAVE_SESSAO_CONTEXTO_MEDIA]
    st.rerun()