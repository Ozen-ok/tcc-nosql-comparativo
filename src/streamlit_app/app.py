# src/streamlit_app/app.py
import streamlit as st

# Define a estrutura de navegação do aplicativo Streamlit.
# Comentamos as páginas que não usaremos inicialmente para focar no fluxo da busca avançada.
paginas = {
    "Informações": [
        st.Page(
            "paginas/home.py",  # Caminho para o script da página inicial.
            title="Início",  # Título exibido na navegação.
            icon="🏠",  # Ícone para a página.
            default=True,  # Define esta como a página padrão ao carregar o app.
            url_path="inicio"  # Caminho na URL para esta página.
        ),
    ],
    "Operações": [
        st.Page(
            "paginas/inserir.py",
            title="Inserir Dados",
            icon="📝",
            url_path="inserir"
        ),
        st.Page(
            "paginas/atualizar.py",
            title="Atualizar Dados",
            icon="🔄",
            url_path="atualizar"
        ),
        st.Page(
            "paginas/deletar.py",
            title="Remover Dados",
            icon="❌",
            url_path="remover"
        ),
        st.Page(
            "paginas/filmes_por_ator.py",
            title="Filmes por ator",
            icon="🔍",
            url_path="filmes-por-ator"
        ),
        st.Page(
            "paginas/busca_avancada.py",  # Caminho para o script da página de busca avançada.
            title="Busca Avançada",  # Título exibido na navegação.
            icon="🧠",  # Ícone para a página.
            url_path="busca-avancada"  # Caminho na URL para esta página.
        ),
        st.Page(
            "paginas/detalhes_filme.py",  # Caminho para o script da página de detalhes do filme.
            title="Detalhes do Filme",  # Título exibido na navegação.
            icon="🎬",  # Ícone para a página.
            url_path="detalhes-filme"  # Caminho na URL para esta página.
        ),
        st.Page(
            "paginas/contar.py",
            title="Contar Registros",
            icon="🔢",
            url_path="contar"
        ),
        st.Page(
            "paginas/media.py",
            title="Contar Média por Gênero",
            icon="🔢",
            url_path="media-por-genero"
        ),
        st.Page(
             "paginas/inserir_base.py",
             title="Inserir Bases de Dados",
             icon="📥",
             url_path="inserir-bases"
        ),
        st.Page(
             "paginas/analise_desempenho.py",
             title="Análise de Desempenho",
             icon="📊",
             url_path="analise-desempenho"
        ),
    ]
}

# Inicializa e executa a navegação do Streamlit com as páginas definidas.
st.navigation(paginas).run()