import pandas as pd
from itertools import combinations

# Lista dos nomes dos atores que te interessam
atores_interesse = [
    "Norman Reedus", "Steven Yeun", "Keanu Reeves", "Ella Purnell",
    "Hailee Steinfeld", "Jeffrey Dean Morgan", "Austin Butler", "Timothée Chalamet",
    "Elle Fanning", "Troy Baker", "Jenna Ortega", "Emma Myers", 
    "Sebastian Stan", "J.K. Simmons", "Kiernan Shipka", "Ana de Armas", 
    "Mikey Madison", "Mia Goth"
]

dados_filtrados= pd.read_csv("dados_filtrados.tsv", sep="\t")

# Filtra os dados apenas para os atores da lista
df_interesse = dados_filtrados[dados_filtrados['nome_ator'].isin(atores_interesse)]

# Armazena pares de atores e os títulos que eles compartilharam
pares_atores = []

# Para cada título, pega todos os atores (de interesse) que atuaram nele
for titulo, grupo in df_interesse.groupby('titulo_id'):
    atores_no_titulo = grupo['nome_ator'].unique()
    if len(atores_no_titulo) > 1:
        for a1, a2 in combinations(sorted(atores_no_titulo), 2):
            pares_atores.append((a1, a2, titulo))

# Cria DataFrame com os pares e títulos
df_pares = pd.DataFrame(pares_atores, columns=['ator1', 'ator2', 'titulo'])

# Agrupa por par e junta os títulos
df_relacoes = (
    df_pares.groupby(['ator1', 'ator2'])
    .agg(num_titulos=('titulo', 'count'), titulos=('titulo', lambda x: list(x)))
    .reset_index()
)

def pegar_ratings(titulos):
    # Garante que 'titulos' seja uma lista de IDs de título
    if isinstance(titulos, str):
        titulos = [titulos]
    
    # Filtra os dados para pegar apenas uma ocorrência por título
    dados_unicos = dados_filtrados[dados_filtrados['titulo_id'].isin(titulos)].drop_duplicates('titulo_id')
    
    # Pega os ratings dos títulos que estão no DataFrame original
    ratings = dados_unicos['nota']

    # Substitui NaN por 0 e filtra apenas ratings válidos
    ratings = ratings.fillna(0)  # Substitui NaN por 0
    # Converte os ratings para uma string separada por vírgula
    return ratings.tolist()

# Aplica a função para pegar os ratings na tabela de relações
df_relacoes['notas'] = df_relacoes['titulos'].apply(pegar_ratings)
df_relacoes['notas'] = df_relacoes['notas'].apply(lambda x: ', '.join(map(str, x)))
df_relacoes['titulos'] = df_relacoes['titulos'].apply(lambda x: ', '.join(x))

print(df_relacoes)
