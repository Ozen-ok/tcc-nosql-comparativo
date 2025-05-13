from components.repetidor import API_URLS
import requests

def tratar_resposta(response):
    if response.status_code == 200:
        try:
            return response.json()
        except ValueError:
            return {"error": "Erro ao decodificar o JSON da resposta."}
    else:
        return {"error": f"Erro {response.status_code}: {response.text}"}

def chamar_api(banco: str, metodo: str, rota: str, params=None, json=None):
    base_url = API_URLS.get(banco)
    if not base_url:
        return {"error": f"Banco '{banco}' não reconhecido."}
    
    url = f"{base_url}/{rota}"
    metodo = metodo.lower()

    try:
        match metodo:
            case "get":
                resp = requests.get(url, params=params)
            case "post":
                resp = requests.post(url, json=json)
            case "put":
                resp = requests.put(url, params=params, json=json)
            case "delete":
                resp = requests.delete(url, params=params)
            case _:
                return {"error": f"Método HTTP não suportado: {metodo}"}
        return tratar_resposta(resp)
    except Exception as e:
        return {"error": str(e)}

# ---------- Funções específicas usando o client genérico ----------

def inserir_filme(banco, titulo_id, titulo, tipo, ano_lancamento, generos, nota, numero_votos, duracao, sinopse):
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
    return chamar_api(banco, "post", "filmes", json=payload)

def deletar_filme(banco, titulo_id_delete):
    return chamar_api(banco, "delete", f"filmes/{titulo_id_delete}")

def contar_filmes_por_ano(banco):
    return chamar_api(banco, "get", "filmes/contagem/ano")

def media_por_genero(banco):
    return chamar_api(banco, "get", "filmes/media-nota/genero")

def busca_avancada(banco, genero, ano_min, nota_min):
    return chamar_api(banco, "get", "filmes/busca-avancada", params={"genero": genero, "ano_min": ano_min, "nota_min": nota_min})

def busca_simples(banco, campo, valor):
    return chamar_api(banco, "get", "filmes/busca-simples", params={"campo": campo, "valor": valor})

def atualizar_campo(banco, titulo_id_update, campo, valor):
    return chamar_api(banco, "put", f"filmes/{titulo_id_update}/{campo}", params={"valor": valor})

def listar_atores_por_filme(banco, titulo_id):
    return chamar_api(banco, "get", f"filmes/{titulo_id}/atores")

def filmes_por_ator(banco, nome_ator):
    return chamar_api(banco, "get", f"filmes/por-ator/{nome_ator}")

def executar_em_todos_bancos(func, *args, **kwargs):
    resultados = {}
    for banco in API_URLS:
        resposta = func(banco, *args, **kwargs)
        resultados[banco] = resposta
    return resultados