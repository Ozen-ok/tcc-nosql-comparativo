import requests
import pandas as pd

API_URL = "http://localhost:8000/mongo"

# Função auxiliar para tratar resposta e verificar erros
def tratar_resposta(response):
    if response.status_code == 200:
        try:
            return response.json()
        except ValueError:
            return {"error": "Erro ao decodificar o JSON da resposta."}
    else:
        return {"error": f"Erro {response.status_code}: {response.text}"}

# Função para inserir filme
def inserir_filme(titulo_id, titulo, ano_lancamento, generos, nota):
    payload = {
        "titulo_id": titulo_id,
        "titulo": titulo,
        "ano_lancamento": ano_lancamento,
        "generos": [g.strip() for g in generos.split(",")],
        "nota": nota
    }
    response = requests.post(f"{API_URL}/filmes", json=payload)
    return tratar_resposta(response)

# Função para buscar filmes por gênero (via path)
def buscar_filmes_por_genero(genero):
    endpoint = f"{API_URL}/filmes/genero/{genero}"
    response = requests.get(endpoint)
    return tratar_resposta(response)

# Função para atualizar nota de um filme
def atualizar_nota(titulo_id_update, nova_nota):
    response = requests.put(f"{API_URL}/filmes/{titulo_id_update}/nota", params={"nova_nota": nova_nota})
    return tratar_resposta(response)

# Função para deletar filme
def deletar_filme(titulo_id_delete):
    response = requests.delete(f"{API_URL}/filmes/{titulo_id_delete}")
    return tratar_resposta(response)

# Função para contar filmes por ano
def contar_filmes_por_ano():
    response = requests.get(f"{API_URL}/filmes/contagem/ano")
    return tratar_resposta(response)

# Função para média por gênero
def media_por_genero():
    response = requests.get(f"{API_URL}/filmes/media-nota/genero")
    return tratar_resposta(response)

# Função para busca avançada com exibição visual
def busca_avancada(genero, ano_min, nota_min):
    params = {
        "genero": genero,
        "ano_min": ano_min,
        "nota_min": nota_min
    }
    response = requests.get(f"{API_URL}/filmes/busca-avancada", params=params)
    
    resposta = tratar_resposta(response)
    if "error" in resposta:
        return resposta
    
    filmes = resposta
    if not filmes:
        return {"warning": "Nenhum resultado encontrado."}
    else:
        df = pd.DataFrame(filmes)
        
        # Garantir que duplicatas sejam removidas
        df = df.drop_duplicates(subset="titulo_id")
        
        # Adicionando a URL dos pôsteres
        base_url_posters = "https://raw.githubusercontent.com/Ozen-ok/imdb/main/data/imagens/posters/"
        df["poster_url"] = df["titulo_id"].apply(lambda tid: base_url_posters + f"{tid}.jpg")
        
        # Caso a estrutura do dataframe não tenha alguns dados, trate isso
        required_columns = ["titulo_id", "titulo", "ano_lancamento", "nota", "generos", "poster_url"]
        for col in required_columns:
            if col not in df.columns:
                df[col] = None  # Defina uma coluna de placeholder caso não exista

        return df.to_dict(orient="records")
