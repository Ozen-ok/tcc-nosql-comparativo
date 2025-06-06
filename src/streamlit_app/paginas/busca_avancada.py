# src/streamlit_app/paginas/busca_avancada.py
import streamlit as st
from components.ui_elements import formulario_busca_avancada, seletor_de_banco_global_sidebar
from components.display_utils import processar_e_exibir_resultados_api
from services.operation_handlers import realizar_busca_avancada
from config.settings import BANCOS_SUPORTADOS # Importa o mapeamento de bancos

# --- LÓGICA DE NAVEGAÇÃO NO TOPO DA PÁGINA ---
if st.session_state.get("navegar_para_detalhes"):
    st.session_state["navegar_para_detalhes"] = False  # Limpa o flag para evitar redirecionamentos múltiplos
    st.switch_page("paginas/detalhes_filme.py")
# --- FIM DA LÓGICA DE NAVEGAÇÃO ---


# Configuração da página (opcional, mas útil)
st.set_page_config(layout="wide", page_title="Busca Avançada de Filmes")
st.title("🔬 Busca Avançada de Filmes")
st.markdown("Refine sua pesquisa utilizando os filtros abaixo e escolha em qual banco de dados deseja buscar ou se deseja buscar em todos.")

# Inicializa placeholder_resultados fora do if
placeholder_resultados = None # <--- INICIALIZE AQUI

# --- 1. Seleção do Banco de Dados ---
# O seletor_de_banco retorna a chave interna do banco (ex: "mongo", "todos")
#banco_chave_selecionada = seletor_de_banco(chave_selectbox="sb_busca_avancada_pagina")
banco_chave_selecionada = seletor_de_banco_global_sidebar()
#st.session_state.banco_chave_selecionada_busca = banco_chave_selecionada # Esta é a chave minúscula, ex: "mongo"

# Opcional: Mostrar qual banco está selecionado (para o usuário ver)
nome_amigavel_banco = BANCOS_SUPORTADOS.get(banco_chave_selecionada, banco_chave_selecionada.capitalize())
#st.sidebar.caption(f"Operando em: {nome_amigavel_banco}") # Mostra na sidebar abaixo do seletor

# --- 2. Formulário de Filtros ---
# A função formulario_busca_avancada retorna os filtros quando o botão de submit do form é clicado
filtros_da_ui = formulario_busca_avancada(chave_form="form_busca_avancada_principal")

# --- 3. Ação de Busca e Exibição de Resultados ---
if filtros_da_ui:
    # O formulário foi submetido, temos os filtros.
    placeholder_resultados = st.empty() # Cria um placeholder para os resultados
    
    st.session_state.banco_em_uso_na_pagina_busca = banco_chave_selecionada # Para display na página atual
    with st.spinner(f"Buscando filmes em '{nome_amigavel_banco}'... Por favor, aguarde."):
        # Chama o operation_handler, que por sua vez chama o api_service
        resposta_do_backend = realizar_busca_avancada(
            filtros_da_ui=filtros_da_ui,
            banco_selecionado=banco_chave_selecionada
        )
        
        # Guarda a resposta completa no session_state para debug ou re-exibição
        st.session_state['ultima_resposta_api_busca_avancada'] = resposta_do_backend

# Exibe os resultados da última busca (se houver)
# Isso permite que os resultados persistam mesmo se houver um rerun por outro motivo
if 'ultima_resposta_api_busca_avancada' in st.session_state:
    
    # A função processar_e_exibir_resultados_api foi criada em display_utils.py
    # Ela saberá como lidar com a resposta (se é erro, dados de um banco, ou de múltiplos)
    if placeholder_resultados: # Limpa o placeholder antes de desenhar
        placeholder_resultados.empty()
    processar_e_exibir_resultados_api(st.session_state['ultima_resposta_api_busca_avancada'], 
    st.session_state['banco_em_uso_na_pagina_busca'])

st.markdown("---")
if st.button("Limpar Resultados da Busca"):
    if 'ultima_resposta_api_busca_avancada' in st.session_state:
        del st.session_state['ultima_resposta_api_busca_avancada']
    if 'form_busca_avancada_principal' in st.session_state: # Limpa o form também, se precisar
        # Você pode querer resetar os campos do form aqui
        pass
    st.rerun()