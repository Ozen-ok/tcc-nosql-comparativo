# src/streamlit_app/paginas/filmes_por_ator.py
import streamlit as st
from components.ui_elements import seletor_ator_para_filmes, seletor_de_banco_global_sidebar
from components.display_utils import processar_e_exibir_resultados_api # Reutiliza para listas de filmes
from services.operation_handlers import handle_listar_filmes_por_ator_operacao
from config.settings import BANCOS_SUPORTADOS, CAMPOS_ORDENACAO_FILMES

# --- LÓGICA DE NAVEGAÇÃO NO TOPO DA PÁGINA ---
if st.session_state.get("navegar_para_detalhes"):
    st.session_state["navegar_para_detalhes"] = False  # Limpa o flag para evitar redirecionamentos múltiplos
    st.switch_page("paginas/detalhes_filme.py")
# --- FIM DA LÓGICA DE NAVEGAÇÃO ---

st.set_page_config(layout="wide", page_title="Filmes por Ator")
st.title("🧑‍🎤 Listar Filmes por Ator")
st.markdown("Selecione um ator e o banco de dados para ver os filmes em que participou.")

if 'banco_selecionado_filmes_ator' not in st.session_state:
    st.session_state.banco_selecionado_filmes_ator = "mongo"
if 'ator_selecionado_id' not in st.session_state: # Supondo que você armazene o ID do ator
    st.session_state.ator_selecionado_id = None


banco_chave_selecionada = seletor_de_banco_global_sidebar()
#st.session_state.banco_selecionado_filmes_ator = banco_chave_selecionada
nome_amigavel_banco = BANCOS_SUPORTADOS.get(banco_chave_selecionada, banco_chave_selecionada.capitalize())
# Opcional: Mostrar qual banco está selecionado (para o usuário ver)
#st.sidebar.caption(f"Operando em: {nome_amigavel_banco}") # Mostra na sidebar abaixo do seletor




# Para o seletor de ator, você precisará de uma forma de obter os IDs dos atores.
# Por agora, vou usar o nome, e o backend terá que lidar com isso ou você busca o ID antes.
# Se ATORES_LISTA em settings.py for só de nomes:
# A função agora retorna {'id': ..., 'nome': ...} ou None
ator_info_selecionada = seletor_ator_para_filmes(chave_selectbox="sb_ator_filmes_page")

if ator_info_selecionada:
    st.session_state.ator_id_selecionado_para_filmes = ator_info_selecionada.get('id')
    st.session_state.ator_nome_selecionado_para_filmes = ator_info_selecionada.get('nome')
else: # Se o placeholder "Selecione um ator..." for escolhido
    st.session_state.ator_id_selecionado_para_filmes = None
    st.session_state.ator_nome_selecionado_para_filmes = None

# Adicionar opções de ordenação e limite (opcional)
col_ord, col_lim = st.columns(2)
with col_ord:
    ordenar_por_label = st.selectbox("Ordenar filmes por:", list(CAMPOS_ORDENACAO_FILMES.keys()), index=2, key="fpa_ordenar_por") # Default 'Nota'
    ordenar_por_key = CAMPOS_ORDENACAO_FILMES[ordenar_por_label]
    ordem_val = -1 if st.radio("Ordem:", ["Decrescente", "Crescente"], index=0, horizontal=True, key="fpa_ordem") == "Decrescente" else 1
with col_lim:
    limite_val = st.number_input("Limite de resultados:", min_value=5, max_value=200, value=40, step=5, key="fpa_limite")


if st.button(f"🎬 Listar Filmes de {st.session_state.get('ator_nome_selecionado_para_filmes')}"):

    st.session_state.banco_em_uso_na_pagina_filmes_ator = banco_chave_selecionada # Para display na página atual
    ator_id_para_api = st.session_state.get('ator_id_selecionado_para_filmes')
    nome_ator_display = st.session_state.get('ator_nome_selecionado_para_filmes', 'Ator não selecionado')

    # AQUI: Se o backend espera ID do ator, você precisaria buscar o ID do ator
    # com base no 'nome_ator_selecionado' antes de chamar o handler.
    # Ex: id_ator_real = buscar_id_ator_pelo_nome_api(nome_ator_selecionado, banco_chave_selecionada)
    # Por ora, passaremos o nome, e o backend/serviço terá que lidar com isso ou você ajusta.
    # No nosso `listar_filmes_por_ator_api` e `servico_listar_filmes_por_ator` já esperamos `id_ator`.
    # Você precisaria de um passo aqui para converter nome em ID.
    # Para simplificar este exemplo, vamos assumir que 'nome_ator_selecionado' é o ID que o backend espera.
    # Se ATORES_LISTA for um dict {nome: id}, você pegaria o id aqui.
    # Supondo que nome_ator_selecionado é o ID por enquanto:
    #nome_ator_selecionado # Substitua isso pela lógica de obter ID do ator

    if ator_id_para_api: # Verifica se temos um ID/nome válido
        placeholder_status = st.empty()
        with st.spinner(f"Buscando filmes de '{nome_ator_display}' em '{nome_amigavel_banco}'..."):
            resposta = handle_listar_filmes_por_ator_operacao(
                id_ator=ator_id_para_api, 
                banco_selecionado=banco_chave_selecionada,
                ordenar_por=ordenar_por_key,
                ordem=ordem_val,
                limite=limite_val
            )
            st.session_state['ultima_resposta_api_filmes_ator'] = resposta
            if placeholder_status: placeholder_status.empty()
    else:
        st.warning("Por favor, selecione um ator.")
        
if 'ultima_resposta_api_filmes_ator' in st.session_state:
    #st.subheader(f"🎬 Filmes encontrados para {st.session_state['ator_nome_selecionado_para_filmes']} em {st.session_state['banco_em_uso_na_pagina_filmes_ator']}")
    
    processar_e_exibir_resultados_api(
        st.session_state['ultima_resposta_api_filmes_ator'],
        st.session_state['banco_em_uso_na_pagina_filmes_ator'],
        pagina_atual_path_para_voltar="paginas/filmes_por_ator.py",
        pagina_atual_label_para_voltar="Filmes por Ator"
    )
    