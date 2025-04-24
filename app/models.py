import pandas as pd
import logging

# Configuração do logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Função para carregar arquivos TSV
def carregar_tsv(caminho, nome):
    logging.info(f"Iniciando processamento de {nome}...")

    colunas_uteis = {
        "name.basics.tsv": ["nconst", "primaryName", "birthYear"],
        "title.basics.tsv": ["tconst", "primaryTitle", "startYear", "titleType", "genres"],
        "title.principals.tsv": ["nconst", "tconst", "characters"]
    }

    usecols = colunas_uteis.get(nome)
    df = pd.read_csv(caminho, sep="\t", dtype=str, na_values="\\N", usecols=usecols)

    logging.info(f"Processamento de {nome} finalizado.")
    return df

# Carregamento dos arquivos
df_names = carregar_tsv("app/datass/name.basics.tsv", "name.basics.tsv")
df_titles = carregar_tsv("app/datass/title.basics.tsv", "title.basics.tsv")
df_principals = carregar_tsv("app/datass/title.principals.tsv", "title.principals.tsv")
df_ratings = carregar_tsv("app/datass/title.ratings.tsv", "title.ratings.tsv")
df_jogos = pd.read_csv("sinopses_igdb_ptbr.csv")
df_filmes = pd.read_csv("sinopses_tmdb_ptbr.csv")

# Lista de atores
atores_interesse = [
    "Norman Reedus", "Steven Yeun", "Keanu Reeves", "Ella Purnell",
    "Hailee Steinfeld", "Jeffrey Dean Morgan", "Austin Butler", "Timothée Chalamet",
    "Elle Fanning", "Troy Baker", "Jenna Ortega", "Emma Myers", 
    "Sebastian Stan", "J.K. Simmons", "Kiernan Shipka", "Ana de Armas", 
    "Mikey Madison", "Mia Goth"
]

# Filtragem de atores
logging.info("Filtrando atores de interesse...")
df_atores = df_names[df_names["primaryName"].isin(atores_interesse)].copy()

# Trabalhos dos atores
logging.info("Filtrando trabalhos dos atores...")
df_filmes_atores = df_principals[df_principals["nconst"].isin(df_atores["nconst"])][["nconst", "tconst", "characters"]].drop_duplicates().copy()
df_filmes_atores["characters"] = df_filmes_atores["characters"].fillna("Desconhecido")

# Filtragem de títulos
logging.info("Filtrando títulos válidos...")
df_titles = df_titles[
    df_titles["titleType"].isin(["movie", "tvSeries", "videoGame"]) |
    df_titles["genres"].str.contains("Animation", na=False)
]
df_titles = df_titles[
    ~df_titles["titleType"].eq("tvEpisode") &
    ~df_titles["genres"].str.contains("Talk-Show|Documentary|News|Reality-TV", na=False)
].copy()

df_titles["startYear"] = pd.to_numeric(df_titles["startYear"], errors="coerce").astype("Int64")

# Lista de títulos e IDs a remover
titulos_para_remover = {
    "BrainSurge", "Going Great", "Grin & Barrett", 
    "Between Two Ferns: The Movie, Sorta Uncut Interviews", 
    "Undead Noise", "GQ Originals"
}

tconst_para_remover = {
    "tt14764514", "tt24811710", "tt27351074", "tt27490333", "tt27844906", "tt27953508",
    "tt28603964", "tt31137273", "tt3171172", "tt33075482", "tt34382912", "tt34385036",
    "tt34728137", "tt35410859", "tt35934883", "tt36161687", "tt4540534", "tt5318678",
    "tt6378702", "tt1071873", "tt12844100", "tt14301644", "tt15824142", "tt20426466",
    "tt23724682", "tt24164454", "tt31049299", "tt31186307", "tt31842499", "tt32268152",
    "tt32321564", "tt36270820", "tt5602478", "tt9155072", "tt35963614", "tt35592048",
    "tt34096616", "tt31382043",
    "tt10671440", "tt13116464", "tt15243816", "tt27368237", "tt35074011", "tt35675345",
    "tt0233085", "tt0428076", "tt0468998", "tt10519134", "tt10837592", 
    "tt11256946", "tt12157412", "tt1267311", "tt1589998", "tt1808291", 
    "tt2201775", "tt2287370", "tt27714581", "tt29348640", "tt30624579", 
    "tt3546436", "tt3704236", "tt3778408", "tt4776800", 
    "tt5533446", "tt7781226", "tt7783892", "tt8228426", 
    "tt9253490", "tt9734602", "tt2124034", "tt1366315",
    "tt32604452", "tt33070884", "tt33628475", "tt4624824", 
    "tt6718412", "tt7526136", "tt0811607", "tt0446520", 
    "tt0868046" , "tt21636064", "tt32117345", "tt0103095", 
    "tt0115380", "tt0229257", "tt10939760", "tt1175292",
    "tt2390684", "tt4841926","tt31179083", "tt15684426",
    "tt30877684", "tt32024266"
}

# Remoção de títulos indesejados
df_titles = df_titles[
    ~df_titles["primaryTitle"].isin(titulos_para_remover) &
    ~df_titles["tconst"].isin(tconst_para_remover)
].copy()

# Merge dos dados principais
logging.info("Unindo dados...")
df_final = df_filmes_atores.merge(df_titles, on="tconst", how="inner") \
                           .merge(df_atores, on="nconst", how="left") \
                           .merge(df_ratings, on="tconst", how="left")

# Correções manuais
correcoes_atores = {
    "Norman Reedus": 1969,
    "Emma Myers": 2002,
    "Austin Butler": 1991
}
for nome, ano in correcoes_atores.items():
    df_final.loc[df_final["primaryName"] == nome, "birthYear"] = ano

correcoes_genero = {
    "Unearthed: Trail of Ibn Battuta": "Action,Adventure",
    "WildStar": "Action,Adventure,Sci-Fi",
    "Generator Rex: Agent of Providence": "Action,Adventure",
    "Lone Echo II": "Adventure,Sci-Fi",
    "Crossing Fields": "Drama"
}
for titulo, genero in correcoes_genero.items():
    df_final.loc[df_final["primaryTitle"] == titulo, "genres"] = genero

# Renomear colunas
df_final.rename(columns={
    "nconst": "actor_id",
    "tconst": "title_id",
    "characters": "character_name",
    "primaryTitle": "title",
    "startYear": "release_year",
    "titleType": "type",
    "genres": "genres",
    "primaryName": "actor_name",
    "birthYear": "actor_birth_year",
    "averageRating": "rating",
    "numVotes": "votes"
}, inplace=True)

# Limpeza do campo character_name
logging.info("Limpando campo characters...")
df_final["character_name"] = (
    df_final["character_name"].astype(str)
    .str.replace(r'[\[\]"]', '', regex=True)
    .str.replace("\\", "'", regex=False)
)

df_final["rating"] = pd.to_numeric(df_final["rating"], errors="coerce")
df_final["votes"] = pd.to_numeric(df_final["votes"], errors="coerce").fillna(0).astype(int)

# Traduções
TRADUCOES_FIXAS = {
    # Gêneros
    "Action": "Ação", "Adventure": "Aventura", "Animation": "Animação",
    "Biography": "Biografia", "Comedy": "Comédia", "Crime": "Crime",
    "Documentary": "Documentário", "Drama": "Drama", "Family": "Família",
    "Fantasy": "Fantasia", "History": "História", "Horror": "Terror",
    "Music": "Música", "Musical": "Musical", "Mystery": "Mistério",
    "Romance": "Romance", "Sci-Fi": "Ficção Científica", "Sport": "Esporte",
    "Thriller": "Suspense", "War": "Guerra", "Western": "Faroeste",

    # Tipos
    "movie": "Filme", "short": "Curta", "tvSeries": "Série de TV",
    "tvMovie": "Filme para TV", "tvEpisode": "Episódio de TV",
    "tvMiniSeries": "Minissérie", "video": "Vídeo", "videoGame": "Jogo",
    "tvSpecial": "Especial de TV"
}

# Traduz tipo e gêneros
logging.info("Aplicando traduções...")
df_final["type"] = df_final["type"].map(TRADUCOES_FIXAS)
df_final["genres"] = df_final["genres"].apply(
    lambda g: ", ".join([TRADUCOES_FIXAS.get(genre.strip(), genre.strip()) for genre in g.split(",")]) if pd.notna(g) else g
)

# Duração (runtimeMinutes)
logging.info("Adicionando duração dos filmes...")
df_titles_com_duracao = pd.read_csv(
    "app/datass/title.basics.tsv",
    sep="\t",
    dtype=str,
    na_values="\\N",
    usecols=["tconst", "runtimeMinutes"]
)
df_titles_com_duracao["runtimeMinutes"] = pd.to_numeric(df_titles_com_duracao["runtimeMinutes"], errors="coerce")
df_final = df_final.merge(df_titles_com_duracao, left_on="title_id", right_on="tconst", how="left")
df_final.drop(columns=["tconst"], inplace=True)

# Correções específicas de dados (runtimeMinutes)
df_final.loc[df_final["title"] == "Providence", "runtimeMinutes"] = 90.0
df_final.loc[df_final["title"] == "Baki the Grappler", "runtimeMinutes"] = 25.0
df_final.loc[df_final["title"] == "Trinity Blood", "runtimeMinutes"] = 25.0
df_final.loc[df_final["title"] == "Mobile Suit Gundam Unicorn", "runtimeMinutes"] = 25.0
df_final.loc[df_final["title"] == "NFL Rush Zone", "runtimeMinutes"] = 25.0

# Renomeia colunas para unificar
df_jogos_renomeado = df_jogos.rename(columns={"titulo": "title"})
df_filmes_renomeado = df_filmes.rename(columns={
    "titulo_id": "title_id",
    "overview": "sinopse",
    "titulo_ptbr": "title_ptbr"
})

# Junta sinopses de jogos (por título)
df_merged = df_final.merge(df_jogos_renomeado[["title", "sinopse"]], on="title", how="left")

# Junta sinopses e títulos pt-BR de filmes/séries (por title_id)
df_final = df_merged.merge(
    df_filmes_renomeado[["title_id", "sinopse", "title_ptbr"]],
    on="title_id",
    how="left",
    suffixes=("", "_filme")
)

# Preenche sinopses faltantes com as dos filmes
df_final["sinopse"] = df_final["sinopse"].fillna(df_final["sinopse_filme"])
df_final.drop(columns=["sinopse_filme"], inplace=True)

# Substitui título original pelo título em pt-BR, se existir
df_final["title"] = df_final["title_ptbr"].combine_first(df_final["title"])
df_final.drop(columns=["title_ptbr"], inplace=True)

# Salvar arquivo final
logging.info("Salvando arquivo final...")
df_final.to_csv("dados_filtrados.tsv", sep="\t", index=False, encoding="utf-8")

print("Arquivo salvo como 'dados_filtrados.tsv' 🚀")
