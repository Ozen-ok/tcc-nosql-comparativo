# src/streamlit_app/paginas/deletar.py
import streamlit as st
from components.ui_elements import formulario_deletar_filme, seletor_de_banco_global_sidebar
from components.display_utils import processar_e_exibir_resposta_simples_api
from services.operation_handlers import handle_remover_filme_operacao
from config.settings import BANCOS_SUPORTADOS

st.set_page_config(layout="wide", page_title="Remover Filme")
st.title("🗑️ Remover Filme")
st.markdown("Selecione o banco e o ID do filme que deseja remover. **Esta ação é irreversível.**")

if 'banco_selecionado_deletar' not in st.session_state:
    st.session_state.banco_selecionado_deletar = "mongo" 

banco_chave_selecionada = seletor_de_banco_global_sidebar()
st.session_state.banco_selecionado_deletar = banco_chave_selecionada
nome_amigavel_banco = BANCOS_SUPORTADOS.get(banco_chave_selecionada, banco_chave_selecionada.capitalize())
# Opcional: Mostrar qual banco está selecionado (para o usuário ver)
#st.sidebar.caption(f"Operando em: {nome_amigavel_banco}") # Mostra na sidebar abaixo do seletor
st.session_state.banco_em_uso_na_pagina_deletar = nome_amigavel_banco




delete_payload = formulario_deletar_filme(chave_form="form_deletar_filme_principal")

if delete_payload:
    placeholder_status = st.empty()
    with st.spinner(f"Removendo filme '{delete_payload['id_filme']}' de '{nome_amigavel_banco}'..."):
        resposta = handle_remover_filme_operacao(
            id_filme=delete_payload["id_filme"],
            banco_selecionado=st.session_state.banco_selecionado_deletar
        )
        st.session_state['ultima_resposta_api_deletar_filme'] = resposta
        if placeholder_status: placeholder_status.empty()
    
if 'ultima_resposta_api_deletar_filme' in st.session_state:
    st.subheader("📋 Resultado da Remoção:")
    processar_e_exibir_resposta_simples_api(
        st.session_state['ultima_resposta_api_deletar_filme'],
        nome_amigavel_banco,
        operacao="Remoção de Filme"
    )