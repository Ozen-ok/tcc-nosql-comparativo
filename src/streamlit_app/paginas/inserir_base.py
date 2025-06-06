# src/streamlit_app/paginas/inserir_base.py
import streamlit as st
from components.ui_elements import formulario_carregar_base, seletor_de_banco, seletor_de_banco_global_sidebar
from components.display_utils import processar_e_exibir_resultados_carga_api
from services.operation_handlers import carregar_base_de_dados_handler #
from config.settings import BANCOS_SUPORTADOS

st.set_page_config(layout="wide", page_title="Carregar Base de Dados")
st.title("💾 Carregar Base de Dados nos Bancos")
st.markdown("""
Esta página permite que você carregue os dados dos arquivos TSV (filmes, atores, elenco)
para os bancos de dados NoSQL selecionados.
Os arquivos devem estar no formato e local esperados (ex: `data/filmes.tsv`).
""")

# --- 1. Seleção do Banco de Dados ---
if 'banco_selecionado_carga' not in st.session_state:
    st.session_state.banco_selecionado_carga = "mongo" # Default


banco_chave_selecionada = seletor_de_banco_global_sidebar()
nome_amigavel_banco = BANCOS_SUPORTADOS.get(banco_chave_selecionada, banco_chave_selecionada.capitalize())
# Opcional: Mostrar qual banco está selecionado (para o usuário ver)
#st.sidebar.caption(f"Operando em: {nome_amigavel_banco}") # Mostra na sidebar abaixo do seletor

st.session_state.banco_selecionado_carga = banco_chave_selecionada
# Para uso no display_utils se o backend retornar dados de um único banco
st.session_state.banco_em_uso_na_pagina_carga = nome_amigavel_banco

# --- 2. Formulário para Caminhos dos Arquivos ---
paths_info = formulario_carregar_base(chave_form="form_carga_base_principal")

# --- 3. Ação de Carregar e Exibição de Resultados ---
if paths_info:
    # O formulário foi submetido e os paths foram retornados
    st.markdown("---")
    placeholder_resultados_carga = st.empty() # Cria um placeholder para os resultados
    
    with st.spinner(f"Iniciando carga da base de dados em '{st.session_state.banco_em_uso_na_pagina_carga}'... Isso pode levar alguns minutos."):
        resposta_do_backend = carregar_base_de_dados_handler(
            filmes_path=paths_info["filmes_path"],
            atores_path=paths_info["atores_path"],
            elenco_path=paths_info["elenco_path"],
            banco_selecionado=st.session_state.banco_selecionado_carga
        )
        # Guarda a resposta completa no session_state para debug ou re-exibição
        st.session_state['ultima_resposta_api_carga_base'] = resposta_do_backend

# Exibe os resultados da última carga (se houver)
if 'ultima_resposta_api_carga_base' in st.session_state:
    if 'placeholder_resultados_carga' in locals() and placeholder_resultados_carga is not None:
        placeholder_resultados_carga.empty() # Limpa o placeholder ANTES de desenhar os novos resultados
    
    st.subheader("📋 Resultados da Operação de Carga:")
    processar_e_exibir_resultados_carga_api(st.session_state['ultima_resposta_api_carga_base'])

st.markdown("---")
if st.button("Limpar Resultados da Carga Anterior"):
    if 'ultima_resposta_api_carga_base' in st.session_state:
        del st.session_state['ultima_resposta_api_carga_base']
    st.rerun()