# paginas/home.py

import streamlit as st

# --- Configuração da Página ---
st.set_page_config(layout="wide", page_title="Início - TCC NoSQL")
st.title("🎓 Estudo de Caso: Bancos de Dados NoSQL")

# --- Introdução ---
st.markdown("""
Bem-vindo à aplicação prática desenvolvida para o Trabalho de Conclusão de Curso: **"Aplicabilidade e desempenho de banco de dados NoSQL em aplicações modernas: um estudo de caso em MongoDB, Cassandra, Neo4j e Redis."** 

Esta plataforma permite não apenas a exploração interativa de uma base de dados de filmes, mas também serve como vitrine para a análise de desempenho comparativo entre quatro das mais populares tecnologias NoSQL do mercado. 
""")

st.info("**Objetivo do Projeto:** Comparar bancos NoSQL em operações de CRUD, agregações e buscas, utilizando uma aplicação moderna com Python, FastAPI e Streamlit para demonstrar a performance e aplicabilidade de cada um. ", icon="🎯")

st.markdown("---")

# --- Os Competidores ---
st.header("Os 4 Competidores: Conheça os Bancos de Dados")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.subheader("🍃 MongoDB")
    st.markdown("Um banco de dados orientado a documentos, conhecido por sua flexibilidade e escalabilidade. Ideal para uma vasta gama de aplicações modernas.")

with col2:
    st.subheader("🗄️ Cassandra")
    st.markdown("Projetado para altíssima disponibilidade e escalabilidade massiva, com uma arquitetura distribuída que não possui um ponto único de falha.")

with col3:
    st.subheader("🕸️ Neo4j")
    st.markdown("O líder em bancos de dados de grafos, otimizado para armazenar, consultar e analisar dados altamente conectados e seus relacionamentos.")

with col4:
    st.subheader("⚡ Redis")
    st.markdown("Um banco de dados em memória extremamente rápido, frequentemente usado como cache, message broker e para estruturas de dados de alta velocidade.")

st.markdown("---")

# --- Como Navegar ---
st.header("🗺️ Como Explorar a Aplicação")
st.markdown("""
Use a barra de navegação à esquerda para acessar as diferentes seções do projeto:

- **📊 Análise de Desempenho:** O coração deste TCC. Veja os gráficos e as análises detalhadas dos testes de performance que executamos em cada banco de dados.
- **⚙️ Operações:** Interaja diretamente com os bancos. Aqui você pode fazer buscas, inserir novos dados, atualizar e remover registros para ver a aplicação em funcionamento.
""")

# --- Detalhes Técnicos ---
with st.expander("Ver Detalhes Técnicos e Estrutura do Projeto"):
    st.subheader("🛠️ Tecnologias e Ferramentas")
    st.markdown("""
    - **Linguagens e Frameworks:** Python, FastAPI, Streamlit
    - **Bancos de Dados NoSQL:** MongoDB, Cassandra, Neo4j, Redis
    - **Infraestrutura:** Docker, Docker Compose
    - **Outras Ferramentas:** Pydantic (validação de dados), Ambientes Virtuais (.venv)
    """)

    st.subheader("🗂️ Estrutura do Projeto")
    st.markdown("O projeto segue o padrão `src-layout` para uma organização de código limpa e escalável.")
    # MUDANÇA AQUI: Código atualizado para refletir sua estrutura real
    st.code("""
tcc-nosql-comparativo/
├── .streamlit/
├── .vscode/
├── assets/
├── data/
│   ├── atores.tsv
│   ├── elenco.tsv
│   └── filmes.tsv
├── src/
│   ├── api/
│   ├── core/
│   ├── databases/
│   ├── models/
│   ├── services/
│   ├── streamlit_app/
│   └── utils/
├── testes/
├── venv_imdb/
├── .env
├── .gitignore
├── docker-compose.yml
├── Dockerfile.assets
├── notas_do_projeto.md
├── README.md
└── requirements.txt
    """, language="bash")