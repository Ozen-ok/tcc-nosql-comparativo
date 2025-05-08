import pandas as pd
import os

def verificar_titulos_sem_imagem_local(caminho_tsv, caminho_imagens):
    # Carrega os dados
    df = pd.read_csv(caminho_tsv, sep='\t')
    
    # Garante que não haja duplicatas
    df = df.drop_duplicates(subset="titulo_id")

    # Verifica se a imagem existe
    def imagem_existe(titulo_id):
        return os.path.isfile(os.path.join(caminho_imagens, f"{titulo_id}.jpg"))

    # Filtra os títulos sem imagem
    df_sem_img = df[~df["titulo_id"].apply(imagem_existe)]

    # Mostra resultado
    print(f"Encontrados {len(df_sem_img)} títulos sem imagem:")
    for _, row in df_sem_img.iterrows():
        print(f"- {row['titulo']} ({row['titulo_id']})")

# Exemplo de uso:
verificar_titulos_sem_imagem_local("data/filmes.tsv", "assets/imagens/posters")
