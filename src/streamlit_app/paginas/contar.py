# src/streamlit_app/paginas/contar.py
import streamlit as st
from components.ui_elements import seletor_de_banco_global_sidebar
from components.display_utils import exibir_grafico_contagem_por_ano
from services.operation_handlers import handle_contar_filmes_por_ano_operacao
from config.settings import BANCOS_SUPORTADOS

st.set_page_config(layout="wide", page_title="Contagem de Filmes por Ano")
st.title("📈 Contagem de Filmes por Ano de Lançamento")
st.markdown("Veja a distribuição da quantidade de filmes ao longo dos anos para o banco selecionado.")

# --- Seleção de Banco (a lógica aqui fica mais simples) ---
banco_global_atual_chave = seletor_de_banco_global_sidebar()
nome_banco_amigavel = BANCOS_SUPORTADOS.get(banco_global_atual_chave, banco_global_atual_chave.capitalize())
#st.sidebar.caption(f"Operando em: {nome_banco_amigavel}")

# --- BOTÃO E LÓGICA DE BUSCA (AQUI ESTÁ A MUDANÇA) ---
if st.button("📊 Gerar Gráfico de Contagem"):
    with st.spinner(f"Calculando contagem em '{nome_banco_amigavel}'..."):
        resposta = handle_contar_filmes_por_ano_operacao(
            banco_selecionado=banco_global_atual_chave
        )
        # Em vez de guardar só a resposta, guardamos um "contexto" completo
        st.session_state['contexto_ultima_contagem'] = {
            "dados_api": resposta, 
            "banco_chave_usado": banco_global_atual_chave # "Carimba" o resultado com o banco usado
        }
        
# --- EXIBIÇÃO DOS RESULTADOS (AQUI ESTÁ A MUDANÇA) ---
# Verifica se existe um contexto de resultado salvo para exibir
if 'contexto_ultima_contagem' in st.session_state:
    # Pega o contexto salvo
    contexto_salvo = st.session_state['contexto_ultima_contagem']
    
    # Extrai os dados e a chave do banco daquele contexto
    resposta_api_salva = contexto_salvo["dados_api"]
    banco_chave_dos_dados_salvos = contexto_salvo["banco_chave_usado"]
    
    # Chama a função de display passando os dados salvos e o banco ao qual eles pertencem
    exibir_grafico_contagem_por_ano(
        resposta_api_completa=resposta_api_salva,
        banco_selecionado_chave=banco_chave_dos_dados_salvos 
    )

# --- 4. (Opcional) Botão para Limpar ---
st.markdown("---")
if st.button("Limpar Gráfico(s)"):
    if 'contexto_ultima_contagem' in st.session_state:
        del st.session_state['contexto_ultima_contagem']
    st.rerun()