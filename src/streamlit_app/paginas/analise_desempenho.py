# src/streamlit_app/paginas/analise_desempenho.py

import streamlit as st
import pandas as pd
import json
import plotly.express as px
import os
import statistics

# --- Configuração da Página ---
st.set_page_config(layout="wide", page_title="Análise de Desempenho")
st.title("🔬 Análise de Desempenho Comparativo dos Bancos NoSQL")
st.markdown("""
Esta página apresenta os resultados consolidados dos testes de performance executados nos quatro bancos de dados. 
Cada cenário de teste avalia uma carga de trabalho diferente, revelando os pontos fortes e fracos de cada arquitetura.
""")
st.markdown("---")

# --- Função Auxiliar para Carregar e Processar os Dados ---
def carregar_e_processar_resultados(filepath):
    if not os.path.exists(filepath):
        st.warning(f"Arquivo de resultados não encontrado: {filepath}")
        return None, None

    try:
        with open(filepath, 'r') as f:
            dados = json.load(f)
        
        # Desconsidera a primeira execução (warm-up) para uma média mais estável
        medias = {banco: statistics.mean(tempos[1:]) for banco, tempos in dados.items() if len(tempos) > 1}
        
        df = pd.DataFrame(list(medias.items()), columns=['Banco', 'Tempo Médio (s)'])
        df['Banco'] = df['Banco'].str.capitalize()
        df = df.sort_values(by='Tempo Médio (s)', ascending=True)
        
        return df, dados
    except Exception as e:
        st.error(f"Erro ao processar o arquivo {filepath}: {e}")
        return None, None

def plotar_grafico(df, titulo):
    if df is not None:
        fig = px.bar(
            df,
            x='Banco',
            y='Tempo Médio (s)',
            title=titulo,
            labels={'Banco': 'Banco de Dados', 'Tempo Médio (s)': 'Tempo Médio (s)'},
            text_auto='.4f',
            color='Banco',
            color_discrete_map={
                'Redis': '#D82C20', 'Mongo': '#4E9D44', 
                'Neo4j': '#008CC1', 'Cassandra': '#1D3A54'
            }
        )
        fig.update_traces(textposition='outside')
        st.plotly_chart(fig, use_container_width=True)

# --- Layout dos Testes ---
testes = {
    "Inserção em Lote": {
        "filepath": "testes/resultados_insercao.json",
        "descricao": "Carga inicial de ~1800 registros (filmes, atores, elenco). Mede a performance de escrita massiva.",
        "analise": {
            "🏆 Vencedor: Redis (Execução Pura)": "Com o pipeline otimizado, o tempo de execução no servidor Redis é de meros **~40ms**. A maior parte do tempo total (~0.43s) é gasta no preparo dos dados em Python, provando que a otimização do cliente é crucial.",
            "🥈 MongoDB (~0.18s)": "Performance de escrita muito rápida e consistente, sendo uma excelente opção para cargas de trabalho mistas.",
            "🥉 Neo4j (~0.21s)": "Ótima performance, mesmo com a complexidade de criar nós e milhares de relacionamentos, demonstrando a eficiência de seu motor transacional para dados conectados.",
            "🏅 Cassandra (~1.87s)": "O mais lento para escritas em nó único, um comportamento esperado de sua arquitetura que prioriza a escalabilidade e a resiliência em detrimento da latência de escrita."
        }
    },
    "Busca Avançada": {
        "filepath": "testes/resultados_busca.json",
        "descricao": "Busca complexa com múltiplos filtros (tipo, ano, múltiplos gêneros, nota). Mede a eficiência dos índices e motores de consulta.",
        "analise": {
            "🏆 Vencedor: Neo4j (~0.014s)": "Imbatível. Uma vez que o plano de consulta é cacheado, sua capacidade de percorrer padrões de dados é superior para leituras complexas.",
            "🥈 MongoDB (~0.05s)": "Extremamente rápido e estável, provando o poder de seus índices secundários para filtrar rapidamente uma grande coleção de documentos.",
            "🥉 Cassandra (~0.05s)": "Surpreendentemente rápido, empatando com o MongoDB. **Importante:** Este resultado se deve à nossa pequena base de dados, onde uma varredura (`ALLOW FILTERING`) seguida de filtro no cliente é performática. Este resultado não escalaria para milhões de registros.",
            "🏅 Redis (~0.11s)": "O mais lento aqui. Sem um motor de busca nativo, a necessidade de buscar cada filme individualmente após uma consulta inicial no índice gerou um alto overhead de rede."
        }
    },
    "Agregação (Média por Gênero)": {
        "filepath": "testes/resultados_media_genero.json",
        "descricao": "Agregação complexa que exige desmembrar um array e calcular uma média. Testa a capacidade analítica de cada banco.",
        "analise": {
            "🏆 Vencedor: Neo4j (~0.01s)": "Novamente o campeão. Operações analíticas são naturais para o Cypher, que resolveu a consulta de forma nativa e eficiente.",
            "🥈 MongoDB (~0.04s)": "O Aggregation Framework mostrou seu poder, realizando toda a operação complexa no lado do servidor com alta performance.",
            "🥉 Cassandra (~0.07s)": "Resultado decente graças à leitura otimizada de apenas duas colunas, com o cálculo sendo feito no cliente. Uma solução viável para datasets menores.",
            "🏅 Redis (~0.23s)": "Sofre com a falta de agregação no servidor, exigindo que todos os dados sejam trazidos para o cliente para serem processados, o que o torna o menos eficiente para esta tarefa."
        }
    },
    "Busca por Relacionamento": {
        "filepath": "testes/resultados_filmes_ator.json",
        "descricao": "Busca de todos os filmes de um ator específico. Testa a eficiência em percorrer relacionamentos 1-N.",
        "analise": {
            "🏆 Vencedor: Neo4j (~0.017s)": "Este é o seu 'habitat natural'. Percorrer um relacionamento existente é a operação mais rápida e fundamental em um banco de grafos.",
            "🥈 Redis (~0.027s)": "A surpresa do teste! Uma performance fantástica graças à **modelagem de dados inteligente**, onde criamos um índice manual (um Set) para responder a essa pergunta específica com altíssima velocidade.",
            "🥉 MongoDB (~0.04s)": "Muito sólido, utilizando o operador `$lookup` para fazer a junção entre as coleções de forma otimizada no servidor.",
            "🏅 Cassandra (~0.06s)": "Também muito rápido, pois a consulta foi modelada exatamente para o seu ponto forte: buscar dados por chave de partição (`ator_id`)."
        }
    }
}

for nome_teste, info in testes.items():
    st.header(f"Teste de Desempenho: {nome_teste}")
    
    col1, col2 = st.columns([2, 1.5])

    with col1:
        st.markdown(f"**Cenário:** {info['descricao']}")
        df_resultado, dados_brutos = carregar_e_processar_resultados(info["filepath"])
        if df_resultado is not None:
            plotar_grafico(df_resultado, f"Tempo Médio - {nome_teste}")

    with col2:
        st.markdown("##### 📝 Análise dos Resultados")
        for titulo_analise, texto_analise in info["analise"].items():
            st.markdown(f"**{titulo_analise}:** {texto_analise}")

    with st.expander("Ver dados brutos do teste (JSON)"):
        st.json(dados_brutos)

    st.markdown("---")


# --- Conclusão Geral ---
st.header("🏁 Conclusões Gerais")
st.markdown("""
A análise dos quatro cenários de teste demonstra de forma conclusiva que **não existe um "banco de dados perfeito"** para todas as situações. A escolha da tecnologia deve ser guiada pela natureza da carga de trabalho principal da aplicação.
- **Neo4j** é a escolha superior para aplicações onde os relacionamentos e as análises de conexões são o coração do negócio.
- **MongoDB** se destaca como um banco de dados de propósito geral extremamente robusto e flexível, com excelente performance tanto em escritas quanto em leituras e agregações complexas.
- **Redis** é imbatível para cenários de altíssima velocidade de escrita e leitura por chave ou por índices pré-calculados, mas não é a ferramenta ideal para consultas analíticas ad-hoc.
- **Cassandra** mostra seu valor em modelos de dados orientados a consulta e brilha em arquiteturas distribuídas (não totalmente explorado neste TCC), onde a resiliência e a escalabilidade de escrita são mais importantes que a latência em um único nó.
""")