import pandas as pd

# Carrega os dados
df_base = pd.read_csv("dados_filtrados.tsv", sep="\t")
df_jogos = pd.read_csv("sinopses_igdb_ptbr.csv")
df_filmes = pd.read_csv("sinopses_tmdb_ptbr.csv")

# Renomeia colunas para unificar
df_jogos_renomeado = df_jogos.rename(columns={"summary_ptbr": "sinopse"})
df_filmes_renomeado = df_filmes.rename(columns={
    "imdb_id": "title_id",
    "overview": "sinopse",
    "title_ptbr": "title_ptbr"
})

# Junta sinopses de jogos (por título)
df_merged = df_base.merge(df_jogos_renomeado[["title", "sinopse"]], on="title", how="left")

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

# Salva resultado
df_final.to_csv("dados_com_sinopses_e_titulos_ptbr.tsv", sep="\t", index=False)

print("✅ Sinopses e títulos pt-BR adicionados com sucesso!")
