import pandas as pd
import logging

# Configuração do logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Função para carregar arquivos TSV
def carregar_tsv(caminho, nome, separador):
    logging.info(f"Iniciando processamento de {nome}...")

    colunas_uteis = {
        "name.basics.tsv": ["nconst", "primaryName", "birthYear"],
        "title.basics.tsv": ["tconst", "primaryTitle", "startYear", "titleType", "genres"],
        "title.principals.tsv": ["nconst", "tconst", "characters"]
    }

    usecols = colunas_uteis.get(nome)
    df = pd.read_csv(caminho, sep=separador, dtype=str, na_values="\\N", usecols=usecols)

    logging.info(f"Processamento de {nome} finalizado.")
    return df

# Carregamento dos arquivos
df_names = carregar_tsv("app/datass/name.basics.tsv", "name.basics.tsv", "\t")
df_titles = carregar_tsv("app/datass/title.basics.tsv", "title.basics.tsv", "\t")
df_principals = carregar_tsv("app/datass/title.principals.tsv", "title.principals.tsv", "\t")
df_ratings = carregar_tsv("app/datass/title.ratings.tsv", "title.ratings.tsv", "\t")
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
    "tt30877684", "tt32024266", "tt31137263", "tt23985892"
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
    "nconst": "ator_id",
    "tconst": "titulo_id",
    "characters": "nome_personagem",
    "primaryTitle": "titulo",
    "startYear": "ano_lancamento",
    "titleType": "tipo",
    "genres": "generos",
    "primaryName": "nome_ator",
    "birthYear": "ano_nascimento",
    "averageRating": "nota",
    "numVotes": "numero_votos",
}, inplace=True)

# Limpeza do campo character_name
logging.info("Limpando campo characters...")
df_final["nome_personagem"] = (
    df_final["nome_personagem"].astype(str)
    .str.replace(r'[\[\]"]', '', regex=True)
    .str.replace("\\", "'", regex=False)
)

df_final["nota"] = pd.to_numeric(df_final["nota"], errors="coerce")
# Substitui valores nulos na coluna 'nota' por 0
df_final["nota"] = df_final["nota"].fillna(0).astype(float)

df_final["numero_votos"] = pd.to_numeric(df_final["numero_votos"], errors="coerce").fillna(0).astype(int)

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
df_final["tipo"] = df_final["tipo"].map(TRADUCOES_FIXAS)
df_final["generos"] = df_final["generos"].apply(
    lambda g: ", ".join([TRADUCOES_FIXAS.get(genre.strip(), genre.strip()) for genre in g.split(",")]) if pd.notna(g) else g
)

correcoes_tipo = {
    "tt4189294": "Curta",
    "tt5371572": "Curta"
}
for titulo_id, tipo in correcoes_tipo.items():
    df_final.loc[df_final["titulo_id"] == titulo_id, "tipo"] = tipo

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
df_final = df_final.merge(df_titles_com_duracao, left_on="titulo_id", right_on="tconst", how="left")
df_final.drop(columns=["tconst"], inplace=True)

# Renomear colunas
df_final.rename(columns={"runtimeMinutes":"duracao"}, inplace=True)

# Substitui 'duracao' por 0 sempre que 'tipo' for 'Jogo'
df_final.loc[df_final["tipo"] == "Jogo", "duracao"] = 0
df_final["duracao"] = df_final["duracao"].fillna(0).astype(float)

# Correções específicas de dados (runtimeMinutes)
df_final.loc[df_final["titulo"] == "Providence", "duracao"] = 90.0
df_final.loc[df_final["titulo"] == "Baki the Grappler", "duracao"] = 25.0
df_final.loc[df_final["titulo"] == "Trinity Blood", "duracao"] = 25.0
df_final.loc[df_final["titulo"] == "Mobile Suit Gundam Unicorn", "duracao"] = 25.0
df_final.loc[df_final["titulo"] == "NFL Rush Zone", "duracao"] = 25.0

# Junta sinopses de jogos (por título)
df_merged = df_final.merge(df_jogos[["titulo", "sinopse"]], on="titulo", how="left")

# Junta sinopses e títulos pt-BR de filmes/séries (por title_id)
df_final = df_merged.merge(
    df_filmes[["titulo_id", "sinopse", "titulo_ptbr"]],
    on="titulo_id",
    how="left",
    suffixes=("", "_filme")
)

# Preenche sinopses faltantes com as dos filmes
df_final["sinopse"] = df_final["sinopse"].fillna(df_final["sinopse_filme"])
df_final.drop(columns=["sinopse_filme"], inplace=True)

# Substitui título original pelo título em pt-BR, se existir
df_final["titulo"] = df_final["titulo_ptbr"].combine_first(df_final["titulo"])
df_final.drop(columns=["titulo_ptbr"], inplace=True)

# Ajustar os tipos de colunas no df_final
df_final["ator_id"] = df_final["ator_id"].astype(str)
df_final["titulo_id"] = df_final["titulo_id"].astype(str)
df_final["nome_personagem"] = df_final["nome_personagem"].astype(str)
df_final["tipo"] = df_final["tipo"].astype(str)
df_final["titulo"] = df_final["titulo"].astype(str)
df_final["generos"] = df_final["generos"].apply(lambda x: x.split(", ") if pd.notna(x) else [])
df_final["nome_ator"] = df_final["nome_ator"].astype(str)
df_final["sinopse"] = df_final["sinopse"].astype(str)


# Salvar arquivo final
logging.info("Salvando arquivo final...")
df_final.to_csv("dados_filtrados.tsv", sep="\t", index=False, encoding="utf-8")

print("Arquivo salvo como 'dados_filtrados.tsv' 🚀")
