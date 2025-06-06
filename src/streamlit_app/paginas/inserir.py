# src/streamlit_app/paginas/inserir.py
import streamlit as st
# Ajuste os caminhos de importação conforme a estrutura do seu projeto Streamlit
# Se 'components', 'services', 'config' estão em subdiretórios de 'streamlit_app'
# e 'paginas' também, então:
from components.ui_elements import formulario_inserir_filme, seletor_de_banco_global_sidebar
from components.display_utils import processar_e_exibir_resposta_simples_api
from services.operation_handlers import handle_inserir_filme_operacao
from config.settings import BANCOS_SUPORTADOS

st.set_page_config(layout="wide", page_title="Inserir Novo Filme")
st.title("➕ Inserir Novo Filme")
st.markdown("Preencha os dados abaixo para adicionar um novo filme ao banco de dados selecionado.")

# Gerenciamento do banco selecionado para esta página
if 'banco_selecionado_pagina_inserir' not in st.session_state:
    st.session_state.banco_selecionado_pagina_inserir = "mongo" # Default

banco_chave_selecionada = seletor_de_banco_global_sidebar()
st.session_state.banco_selecionado_pagina_inserir = banco_chave_selecionada
nome_amigavel_banco = BANCOS_SUPORTADOS.get(banco_chave_selecionada, banco_chave_selecionada.capitalize())
# Opcional: Mostrar qual banco está selecionado (para o usuário ver)
#st.sidebar.caption(f"Operando em: {nome_amigavel_banco}") # Mostra na sidebar abaixo do seletor

st.markdown(f"Você está inserindo dados no banco: **{nome_amigavel_banco}**")
st.markdown("---")

# Chama o formulário para obter os dados do filme
# O formulário_inserir_filme retorna o dicionário do filme se submetido e válido, senão None
dados_do_novo_filme = formulario_inserir_filme(chave_form="form_filme_principal_inserir")

if dados_do_novo_filme:
    # Se o formulário foi submetido e os dados são válidos
    placeholder_insercao = st.empty() # Para feedback visual durante a operação
    with st.spinner(f"Inserindo o filme '{dados_do_novo_filme.get('titulo', 'Novo Filme')}' em {banco_chave_selecionada}..."):
        # Chama o handler da operação, que chama o api_service
        resposta_api_insercao = handle_inserir_filme_operacao(
            dados_filme=dados_do_novo_filme,
            banco_selecionado=st.session_state.banco_selecionado_pagina_inserir
        )
        # Guarda a resposta no session_state para persistir entre reruns se necessário
        st.session_state['ultima_resposta_insercao_filme'] = resposta_api_insercao
        if placeholder_insercao: # Limpa o placeholder
            placeholder_insercao.empty()

# Exibe o resultado da última operação de inserção, se houver
if 'ultima_resposta_insercao_filme' in st.session_state:
    st.subheader("📋 Resultado da Inserção:")
    processar_e_exibir_resposta_simples_api(
        st.session_state['ultima_resposta_insercao_filme'],
        banco_chave_selecionada, # Passa o nome amigável do banco para a função de display
        operacao="Inserção de Filme" # Informa o tipo de operação para a mensagem
    )
    # Botão para limpar o resultado e permitir nova inserção sem ver o resultado antigo
    if st.button("Limpar Resultado e Inserir Novo Filme"):
        del st.session_state['ultima_resposta_insercao_filme']
        st.rerun() # Força um rerun para limpar o formulário (se ele não limpar por si só)

st.markdown("---")
st.caption("Nota: O 'ID do Título' será usado como a chave primária `_id` no MongoDB.")