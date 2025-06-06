import streamlit as st

def definir_pagina(pagina, **kwargs):
    st.session_state["pagina"] = pagina
    for k, v in kwargs.items():
        st.session_state[k] = v
    st.rerun()

# Função de paginação reutilizável
def paginar_resultados(lista, tamanho_pagina=10, chave="pagina_resultado"):
    if chave not in st.session_state:
        st.session_state[chave] = 1
    pagina_atual = st.session_state[chave]
    total_paginas = (len(lista) + tamanho_pagina - 1) // tamanho_pagina

    inicio = (pagina_atual - 1) * tamanho_pagina
    fim = inicio + tamanho_pagina

    st.markdown(f"📄 Página {pagina_atual} de {total_paginas}")

    col1, col2, col3 = st.columns([1, 6, 1])
    with col1:
        if st.button("⬅️ Anterior", key=f"{chave}_anterior", disabled=pagina_atual == 1):
            st.session_state[chave] = pagina_atual - 1
            st.rerun()
    with col3:
        if st.button("Próxima ➡️", key=f"{chave}_proxima", disabled=pagina_atual == total_paginas):
            st.session_state[chave] = pagina_atual + 1
            st.rerun()

    return lista[inicio:fim]