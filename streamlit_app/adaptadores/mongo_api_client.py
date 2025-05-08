import requests
import pandas as pd
from components.repetidor import MONGO_API_URL

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
def inserir_filme(titulo_id, titulo, tipo, ano_lancamento, generos, nota, numero_votos, duracao, sinopse):
    payload = {
        "titulo_id": titulo_id,
        "titulo": titulo,
        "tipo": tipo,
        "ano_lancamento": ano_lancamento,
        "generos": [g.strip() for g in generos.split(",")],
        "nota": nota,
        "numero_votos": numero_votos,
        "duracao": duracao,
        "sinopse": sinopse
    }
    response = requests.post(f"{MONGO_API_URL}/filmes", json=payload)
    return tratar_resposta(response)


# Função para buscar filmes por gênero (via path)
def buscar_filmes_por_genero(genero):
    endpoint = f"{MONGO_API_URL}/filmes/genero/{genero}"
    response = requests.get(endpoint)
    return tratar_resposta(response)

# Função para atualizar nota de um filme
def atualizar_nota(titulo_id_update, nova_nota):
    response = requests.put(f"{MONGO_API_URL}/filmes/{titulo_id_update}/nota", params={"nova_nota": nova_nota})
    return tratar_resposta(response)

# Função para deletar filme
def deletar_filme(titulo_id_delete):
    response = requests.delete(f"{MONGO_API_URL}/filmes/{titulo_id_delete}")
    return tratar_resposta(response)

# Função para contar filmes por ano
def contar_filmes_por_ano():
    response = requests.get(f"{MONGO_API_URL}/filmes/contagem/ano")
    return tratar_resposta(response)

# Função para média por gênero
def media_por_genero():
    response = requests.get(f"{MONGO_API_URL}/filmes/media-nota/genero")
    return tratar_resposta(response)

def busca_avancada(genero, ano_min, nota_min):
    params = {
        "genero": genero,
        "ano_min": ano_min,
        "nota_min": nota_min
    }
    response = requests.get(f"{MONGO_API_URL}/filmes/busca-avancada", params=params)
    return tratar_resposta(response)



