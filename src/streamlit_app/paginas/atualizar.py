# src/streamlit_app/paginas/atualizar.py
import streamlit as st
from components.ui_elements import formulario_interativo_atualizar_filme, seletor_de_banco_global_sidebar
from components.display_utils import processar_e_exibir_resposta_simples_api
from services.operation_handlers import handle_atualizar_filme_operacao
from config.settings import BANCOS_SUPORTADOS

st.set_page_config(layout="wide", page_title="Atualizar Filme")
st.title("✏️ Atualizar Campo de um Filme")
st.markdown("Selecione o banco, o filme pelo ID, o campo que deseja alterar e o novo valor. O campo de 'Novo Valor' mudará dinamicamente conforme sua seleção.")

# Seleção de Banco
if 'banco_sel_atualizar_page' not in st.session_state: # Chave de session_state específica para esta página
    st.session_state.banco_sel_atualizar_page = "mongo" 

banco_chave_selecionada = seletor_de_banco_global_sidebar()
# 'banco_escolhido_chave_interna' agora contém "mongo", "cassandra", "todos", etc.

# Opcional: Mostrar qual banco está selecionado (para o usuário ver)
nome_amigavel_banco = BANCOS_SUPORTADOS.get(banco_chave_selecionada, banco_chave_selecionada.capitalize())
#st.sidebar.caption(f"Operando em: {nome_amigavel_banco}") # Mostra na sidebar abaixo do seletor

# 'banco_escolhido_chave_interna' agora contém "mongo", "cassandra", "todos", etc.
st.session_state.banco_sel_atualizar_page = banco_chave_selecionada

#banco_chave_selecionada = seletor_de_banco(chave_selectbox="sb_atualizar_main_page")

st.markdown(f"Atualizando dados no banco: **{nome_amigavel_banco}**")
st.markdown("---")

# Chama o formulário interativo. Ele retorna o payload SÓ quando o botão de ação é clicado.
update_payload = formulario_interativo_atualizar_filme(prefixo_chave="form_atualizar_filme")

if update_payload: 
    # Se o botão "Atualizar Filme Agora" foi clicado e o payload é válido:
    placeholder_status = st.empty()
    with st.spinner(f"Atualizando campo '{update_payload['campo_para_atualizar']}' do filme '{update_payload['id_filme']}' em '{nome_amigavel_banco}'..."):
        resposta = handle_atualizar_filme_operacao(
            id_filme=update_payload["id_filme"],
            campo=update_payload["campo_para_atualizar"],
            novo_valor=update_payload["novo_valor"],
            banco_selecionado=st.session_state.banco_sel_atualizar_page
        )
        st.session_state['ultima_resposta_api_atualizar_filme_page'] = resposta # Chave específica
        if placeholder_status: placeholder_status.empty()
    
if 'ultima_resposta_api_atualizar_filme_page' in st.session_state:
    st.subheader("📋 Resultado da Atualização:")
    processar_e_exibir_resposta_simples_api(
        st.session_state['ultima_resposta_api_atualizar_filme_page'], 
        nome_amigavel_banco, 
        operacao="Atualização de Filme"
    )
    # Botão para limpar o resultado e permitir nova operação
    if st.button("Limpar Resultado da Atualização"):
        del st.session_state['ultima_resposta_api_atualizar_filme_page']
        # Você pode querer limpar os campos do formulário no session_state também:
        # del st.session_state[f"form_atualizar_filme_id_filme_val"]
        # del st.session_state[f"form_atualizar_filme_campo_legivel_val"]
        # E todos os f"{prefixo_chave}_novo_valor_dinamico_{campo_interno}"
        st.rerun()