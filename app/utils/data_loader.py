# utils/data_loader.py
import pandas as pd

def carregar_dados(caminho: str):
    """
    Função para carregar a base de dados .tsv (Tab-Separated Values)
    """
    try:
        # Carrega o arquivo TSV usando o Pandas
        dados = pd.read_csv("data/dados_filtrados.tsv", sep='\t')
        print(f"Dados carregados com sucesso! {dados.shape[0]} linhas e {dados.shape[1]} colunas.")
        return dados
    except Exception as e:
        print(f"Erro ao carregar os dados: {e}")
        return None
