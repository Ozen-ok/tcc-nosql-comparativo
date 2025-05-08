import pandas as pd

# Carregar o arquivo original
df = pd.read_csv('dados_filtrados.tsv', sep='\t')

# ----------- filmes.tsv -----------
filmes = df[['titulo_id', 'titulo', 'tipo', 'ano_lancamento', 'generos', 'nota', 'numero_votos', 'duracao', 'sinopse']].drop_duplicates(subset='titulo_id')
filmes.to_csv('filmes.tsv', sep='\t', index=False)

# ----------- atores.tsv -----------
atores = df[['ator_id', 'nome_ator', 'ano_nascimento']].drop_duplicates(subset='ator_id')
atores.to_csv('atores.tsv', sep='\t', index=False)

# ----------- elenco.tsv -----------
elenco = df[['ator_id', 'titulo_id', 'nome_personagem']].drop_duplicates()
elenco.to_csv('elenco.tsv', sep='\t', index=False)

print("✅ Arquivos normalizados salvos: filmes.tsv, atores.tsv e elenco.tsv")
