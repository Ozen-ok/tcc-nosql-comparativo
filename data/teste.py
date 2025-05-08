import pandas as pd

# Caminho do seu arquivo TSV
caminho_arquivo = "filmes.tsv"

# Lê o arquivo TSV
df = pd.read_csv(caminho_arquivo, sep='\t')

# Mostra os valores únicos da coluna 'tipo'
valores_unicos = df["tipo"].unique()

print("Valores únicos na coluna 'tipo':")
for valor in valores_unicos:
    print(f"- {valor}")