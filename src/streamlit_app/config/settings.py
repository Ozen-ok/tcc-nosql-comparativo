# src/streamlit_app/config/settings.py
FASTAPI_BASE_URL = "http://api:8000/api/v1"  # Endpoint base da sua API FastAPI (vamos planejar que ela terá /api/v1)

GENEROS_LISTA = [
    "Ação", "Aventura", "Animação", "Biografia", "Comédia", "Crime",
    "Documentário", "Drama", "Família", "Fantasia", "História", "Terror",
    "Música", "Musical", "Mistério", "Romance", "Ficção Científica",
    "Esporte", "Suspense", "Guerra", "Faroeste"
]

#ATORES_LISTA = [
#    "Norman Reedus", "Steven Yeun", "Keanu Reeves", "Ella Purnell",
#    "Hailee Steinfeld", "Jeffrey Dean Morgan", "Austin Butler", "Timothée Chalamet",
#    "Elle Fanning", "Troy Baker", "Jenna Ortega", "Emma Myers",
#    "Sebastian Stan", "J.K. Simmons", "Kiernan Shipka", "Ana de Armas",
#    "Mikey Madison", "Mia Goth"
#]

ATORES_LISTA = [
    {'id': 'nm0000206', 'nome': 'Keanu Reeves'},
    {'id': 'nm0604742', 'nome': 'Jeffrey Dean Morgan'},
    {'id': 'nm0799777', 'nome': 'J.K. Simmons'},
    {'id': 'nm0005342', 'nome': 'Norman Reedus'},
    {'id': 'nm1684869', 'nome': 'Troy Baker'},
    {'id': 'nm1102577', 'nome': 'Elle Fanning'},
    {'id': 'nm1659221', 'nome': 'Sebastian Stan'},
    {'id': 'nm1869101', 'nome': 'Ana de Armas'},
    {'id': 'nm2581521', 'nome': 'Austin Butler'},
    {'id': 'nm2215143', 'nome': 'Kiernan Shipka'},
    {'id': 'nm3154303', 'nome': 'Timothée Chalamet'},
    {'id': 'nm3081796', 'nome': 'Steven Yeun'},
    {'id': 'nm3480246', 'nome': 'Ella Purnell'},
    {'id': 'nm5301405', 'nome': 'Mia Goth'},
    {'id': 'nm4911194', 'nome': 'Jenna Ortega'},
    {'id': 'nm2794962', 'nome': 'Hailee Steinfeld'},
    {'id': 'nm5700898', 'nome': 'Mikey Madison'},
    {'id': 'nm3528821', 'nome': 'Emma Myers'},
]

CAMPOS_ORDENACAO_FILMES = {
    "Título": "titulo",
    "Ano de Lançamento": "ano_lancamento",
    "Nota": "nota",
    "Número de Votos": "numero_votos",
    "Duração": "duracao"
}

# Mapeamento de bancos para exibição, se necessário
BANCOS_SUPORTADOS = {
    "mongo": "MongoDB",
    "cassandra": "Cassandra",
    "neo4j": "Neo4j",
    "redis": "Redis",
    "todos": "Todos os Bancos"
}

CAMPOS_ATUALIZAVEIS_FILME = {
    "Título": "titulo",
    "Tipo": "tipo",
    "Ano de Lançamento": "ano_lancamento",
    "Gêneros": "generos", # Lembre-se que este é uma lista
    "Nota": "nota",
    "Número de Votos": "numero_votos",
    "Duração": "duracao",
    "Sinopse": "sinopse"
}