# src/streamlit_app/services/api_service.py
import requests
from typing import Any, Dict, List, Optional # Adicionar typing
from config.settings import FASTAPI_BASE_URL


# src/streamlit_app/services/api_service.py
# ... (imports e outras funções)

def _make_request(method: str, endpoint: str, params: dict = None, json_data: dict = None) -> dict:
    url = f"{FASTAPI_BASE_URL}/{endpoint.lstrip('/')}"
    
    #print(f"API Request: {method} {url} | Params: {params} | JSON Body: {json_data}")

    try:
        response = requests.request(method, url, params=params, json=json_data, timeout=30)

        # DEBUG: Ver o status e o texto da resposta bruta
        print(f"DEBUG API Response: Status {response.status_code}, URL: {response.url}")
        #if 200 <= response.status_code < 300:
        #    print(f"DEBUG API Response Text (2xx): '{response.text}'") # <--- IMPORTANTE!

        response.raise_for_status() 
        return response.json()
        
    except requests.exceptions.HTTPError as http_err:
        error_status_code = http_err.response.status_code
        error_detail_from_server = str(http_err) 
        try:
            error_content = http_err.response.json()
            if isinstance(error_content, dict):
                if "detail" in error_content: error_detail_from_server = error_content["detail"]
                elif "message" in error_content: error_detail_from_server = error_content["message"]
                elif "error" in error_content and isinstance(error_content["error"], str): error_detail_from_server = error_content["error"]
        except ValueError:
            error_detail_from_server = http_err.response.text if http_err.response.text else str(http_err)
        print(f"API Error Response: Status {error_status_code}, Detail: {error_detail_from_server}")
        return {"error": f"Erro HTTP ({error_status_code}): {error_detail_from_server}", "status_code": error_status_code}
    
    except requests.exceptions.Timeout:
        print(f"API Request Timeout: {method} {url}")
        return {"error": "A requisição para a API demorou muito para responder (timeout).", "status_code": "TIMEOUT"}
    
    except requests.exceptions.RequestException as req_err:
        print(f"API Request Exception: {method} {url}, Error: {req_err}")
        return {"error": f"Erro na requisição para a API: {req_err}", "status_code": "REQUEST_ERROR"}
    
    except ValueError as json_decode_err: 
        print(f"API JSON Decode Error: {method} {url}, Error: {json_decode_err}")
        return {"error": f"Falha ao decodificar a resposta JSON do servidor (resposta 2xx não era JSON válido): {json_decode_err}", "status_code": "JSON_DECODE_ERROR_2XX"}

# ... (resto do api_service.py)
        
    except requests.exceptions.HTTPError as http_err:
        # Entra aqui se response.raise_for_status() levantou um erro (4xx ou 5xx)
        error_status_code = http_err.response.status_code
        # Começa com a mensagem de erro do próprio 'requests' que inclui o status code
        error_detail_from_server = str(http_err) 

        # Tenta pegar uma mensagem de erro mais específica do corpo da resposta JSON do backend
        # O backend, via resposta_erro, envia {"detail": "mensagem do erro"}
        try:
            error_content = http_err.response.json() # Tenta decodificar o corpo do erro como JSON
            
            if isinstance(error_content, dict):
                # Prioriza a chave 'detail' que o FastAPI e nosso 'resposta_erro' usam
                if "detail" in error_content:
                    error_detail_from_server = error_content["detail"]
                # Fallback para outras chaves comuns de mensagem de erro, se 'detail' não existir
                elif "message" in error_content:
                    error_detail_from_server = error_content["message"]
                elif "error" in error_content and isinstance(error_content["error"], str):
                    error_detail_from_server = error_content["error"]
            # Se error_content não for dict ou não tiver as chaves esperadas, error_detail_from_server continua como str(http_err)
        except ValueError: # Se o corpo do erro não for JSON, usa o texto bruto da resposta ou str(http_err)
            error_detail_from_server = http_err.response.text if http_err.response.text else str(http_err)
        
        # Retorna um dicionário de erro padronizado para o Streamlit
        # Inclui a mensagem formatada e o status_code original do erro HTTP
        print(f"API Error Response: Status {error_status_code}, Detail: {error_detail_from_server}") # Log para o console do Streamlit
        return {"error": f"Erro HTTP ({error_status_code}): {error_detail_from_server}", "status_code": error_status_code}
    
    except requests.exceptions.Timeout:
        print(f"API Request Timeout: {method} {url}")
        return {"error": "A requisição para a API demorou muito para responder (timeout).", "status_code": "TIMEOUT"}
    
    except requests.exceptions.RequestException as req_err: # Outros erros de request (DNS, conexão recusada, etc.)
        print(f"API Request Exception: {method} {url}, Error: {req_err}")
        return {"error": f"Erro na requisição para a API: {req_err}", "status_code": "REQUEST_ERROR"}
    
    except ValueError as json_decode_err: # Erro ao decodificar JSON de uma resposta 2xx que não era JSON
        # Isso é menos comum se a API sempre retorna JSON ou um erro HTTP.
        print(f"API JSON Decode Error: {method} {url}, Error: {json_decode_err}")
        return {"error": f"Falha ao decodificar a resposta JSON do servidor (resposta 2xx não era JSON válido): {json_decode_err}", "status_code": "JSON_DECODE_ERROR_2XX"}

# --- Operações de Leitura (GET) ---
def buscar_filmes_avancado(filtros_busca: dict, banco_alvo: str):
    params = {"banco": banco_alvo}
    return _make_request("POST", "filmes/busca-avancada", params=params, json_data=filtros_busca)

def buscar_filmes_simples(campo: str, valor: any, banco_alvo: str, ordenar_por: str = "nota", ordem: int = -1):
    params = {
        "campo": campo,
        "valor": valor,
        "ordenar_por": ordenar_por,
        "ordem": ordem,
        "banco": banco_alvo
    }
    return _make_request("GET", "filmes/busca-simples", params=params)

def buscar_filme_por_id(titulo_id: str, banco_alvo: str):
    # Este endpoint precisará ser criado no backend se não existir de forma genérica
    params = {"banco": banco_alvo}
    return _make_request("GET", f"filmes/{titulo_id}", params=params)

def listar_atores_de_filme(titulo_id: str, banco_alvo: str):
    params = {"banco": banco_alvo}
    return _make_request("GET", f"filmes/{titulo_id}/atores", params=params)

def listar_filmes_por_ator_api(nome_ator: str, banco_alvo: str, ordenar_por: Optional[str] = None, ordem: Optional[int] = None, limite: Optional[int] = None):
    """Lista filmes de um ator específico."""
    # O endpoint do backend espera id_ator no path.
    # Os outros são query params.
    params_query = {"banco": banco_alvo}
    if ordenar_por: params_query["ordenar_por"] = ordenar_por
    if ordem is not None: params_query["ordem"] = ordem
    if limite is not None: params_query["limite"] = limite
    return _make_request("GET", f"atores/{nome_ator}/filmes", params=params_query)

def contar_filmes_por_ano(banco_alvo: str):
    params = {"banco": banco_alvo}
    return _make_request("GET", "analytics/filmes/contagem-por-ano", params=params) # Rota sugerida

def calcular_media_notas_por_genero(banco_alvo: str):
    params = {"banco": banco_alvo}
    return _make_request("GET", "analytics/filmes/media-notas-por-genero", params=params) # Rota sugerida

# --- Operações de Escrita (POST, PUT, DELETE) ---
def carregar_base_completa(filmes_path: str, atores_path: str, elenco_path: str, banco_alvo: str):
    payload = {
        "filmes_path": filmes_path,
        "atores_path": atores_path,
        "elenco_path": elenco_path
    }
    params = {"banco": banco_alvo}
    return _make_request("POST", "admin/carregar-base", params=params, json_data=payload) # Rota sugerida

def inserir_novo_filme_api(dados_filme: dict, banco_alvo: str):
    """Envia uma requisição para inserir um novo filme no backend."""
    params = {"banco": banco_alvo} # O banco vai como query parameter
    # O 'dados_filme' (payload) vai como corpo JSON da requisição POST
    # O endpoint no backend deve esperar um modelo Pydantic compatível com 'dados_filme'
    return _make_request("POST", "filmes", params=params, json_data=dados_filme)

def atualizar_filme_existente(titulo_id: str, campo: str, novo_valor: any, banco_alvo: str):
    payload = {"campo": campo, "novo_valor": novo_valor} # Ou o backend pode esperar o payload com o campo direto
    params = {"banco": banco_alvo}
    return _make_request("PUT", f"filmes/{titulo_id}", params=params, json_data=payload)


def remover_filme_por_id(titulo_id: str, banco_alvo: str):
    params = {"banco": banco_alvo}
    return _make_request("DELETE", f"filmes/{titulo_id}", params=params)


