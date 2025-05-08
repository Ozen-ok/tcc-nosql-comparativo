import requests
import pandas as pd
from components.repetidor import CASSANDRA_API_URL

# Função auxiliar para tratar resposta e verificar erros
def tratar_resposta(response):
    if response.status_code == 200:
        try:
            return response.json()
        except ValueError:
            return {"error": "Erro ao decodificar o JSON da resposta."}
    else:
        return {"error": f"Erro {response.status_code}: {response.text}"}

# ---------- INSERÇÕES ----------

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
    response = requests.post(f"{CASSANDRA_API_URL}/filmes", json=payload)
    return tratar_resposta(response)

def inserir_ator(ator_id, nome_ator, ano_nascimento):
    payload = {
        "ator_id": ator_id,
        "nome_ator": nome_ator,
        "ano_nascimento": ano_nascimento
    }
    response = requests.post(f"{CASSANDRA_API_URL}/atores", json=payload)
    return tratar_resposta(response)

def inserir_elenco(ator_id, titulo_id, nome_personagem):
    payload = {
        "ator_id": ator_id,
        "titulo_id": titulo_id,
        "nome_personagem": nome_personagem
    }
    response = requests.post(f"{CASSANDRA_API_URL}/elenco", json=payload)
    return tratar_resposta(response)

# ---------- CONSULTAS ----------

def buscar_filmes_por_genero(genero):
    endpoint = f"{CASSANDRA_API_URL}/filmes/genero/{genero}"
    response = requests.get(endpoint)
    return tratar_resposta(response)

def busca_avancada(genero, ano_min, nota_min):
    params = {
        "genero": genero,
        "ano_min": ano_min,
        "nota_min": nota_min
    }
    response = requests.get(f"{CASSANDRA_API_URL}/filmes/busca-avancada", params=params)
    return tratar_resposta(response)


# ---------- ATUALIZAÇÃO / REMOÇÃO ----------

def atualizar_nota(titulo_id_update, nova_nota):
    response = requests.put(
        f"{CASSANDRA_API_URL}/filmes/{titulo_id_update}/nota",
        params={"nova_nota": nova_nota}
    )
    return tratar_resposta(response)

def deletar_filme(titulo_id_delete):
    response = requests.delete(f"{CASSANDRA_API_URL}/filmes/{titulo_id_delete}")
    return tratar_resposta(response)

# ---------- AGREGAÇÕES ----------

def contar_filmes_por_ano():
    response = requests.get(f"{CASSANDRA_API_URL}/filmes/contagem/ano")
    return tratar_resposta(response)

def media_por_genero():
    response = requests.get(f"{CASSANDRA_API_URL}/filmes/media-nota/genero")
    return tratar_resposta(response)
