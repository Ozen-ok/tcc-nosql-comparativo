import pandas as pd

dados_filtrados= pd.read_csv("dados_filtrados.tsv", sep="\t")

print(dados_filtrados.isna().sum())


