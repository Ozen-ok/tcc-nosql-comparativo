# src/streamlit_app/paginas/detalhes_filme.py
import streamlit as st
from services.operation_handlers import obter_detalhes_filme_handler, obter_atores_do_filme_handler
from components.display_utils import renderizar_detalhes_do_filme_completo
from config.settings import BANCOS_SUPORTADOS

st.set_page_config(layout="wide")

# Verifica se o ID do filme e o banco foram passados via session_state
# (A página de busca avançada deve definir 'filme_id_selecionado' e 'banco_filme_selecionado')
id_filme_para_detalhes = st.session_state.get("filme_id_selecionado")
banco_chave_para_detalhes = st.session_state.get("banco_filme_selecionado")

# ---- DEBUG ----
#st.write("DEBUG: ID do Filme Selecionado:", id_filme_para_detalhes)
#st.write("DEBUG: Banco do Filme Selecionado:", banco_chave_para_detalhes)
# ---- FIM DEBUG ----


pagina_anterior_path = st.session_state.get("pagina_anterior_detalhes", "paginas/busca_avancada.py")
label_pagina_anterior = st.session_state.get("label_pagina_anterior_detalhes", "Voltar para Busca Avançada")


if not id_filme_para_detalhes or not banco_chave_para_detalhes:
    st.warning("Nenhum filme selecionado para exibir detalhes ou banco não especificado.")
    st.markdown("Por favor, selecione um filme a partir da página de busca.")
    if st.button(f"⬅️ {label_pagina_anterior}"):
        st.switch_page(pagina_anterior_path)
else:
    nome_banco_amigavel = BANCOS_SUPORTADOS.get(banco_chave_para_detalhes, banco_chave_para_detalhes.capitalize())
    
    # Placeholders para as respostas da API
    detalhes_filme = None
    atores_do_filme = None

    with st.spinner(f"Carregando detalhes do filme de ID '{id_filme_para_detalhes}' em {nome_banco_amigavel}..."):
        # 1. Buscar detalhes do filme
        detalhes_filme = obter_detalhes_filme_handler(
            id_filme=id_filme_para_detalhes,
            banco_selecionado=banco_chave_para_detalhes
        )
        
        # 2. Buscar atores do filme (pode ser feito em paralelo se o backend suportar ou sequencialmente)
        if detalhes_filme and "error" not in detalhes_filme: # Só busca atores se os detalhes do filme foram ok
            atores_do_filme = obter_atores_do_filme_handler(
                id_filme=id_filme_para_detalhes,
                banco_selecionado=banco_chave_para_detalhes
            )
        elif not detalhes_filme: # Se detalhes_filme for None ou vazio
            detalhes_filme = {"error": "Resposta vazia do serviço de detalhes do filme."}


    # 3. Renderizar tudo
    if detalhes_filme: # Garante que temos algo para passar, mesmo que seja um erro
        if st.button(f"⬅️ {label_pagina_anterior}"):
            st.switch_page(pagina_anterior_path)
            
        renderizar_detalhes_do_filme_completo(
            detalhes_filme_resposta=detalhes_filme,
            atores_filme_resposta=atores_do_filme if atores_do_filme else {"error": "Atores não buscados devido a erro anterior ou falta de dados."},
            nome_banco_amigavel=nome_banco_amigavel
        )
    else:
        st.error("Não foi possível obter dados para exibir os detalhes do filme.")

    