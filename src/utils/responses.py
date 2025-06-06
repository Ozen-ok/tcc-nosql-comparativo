import inspect
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from functools import wraps
import traceback

def resposta_sucesso(mensagem: str, dados: dict = None):
    response = {"status": "sucesso", "mensagem": mensagem}
    if dados:
        response.update(dados)
    return response  # Retorna o dicionário completo, não apenas a mensagem


# Versão sugerida para src/utils/responses.py

# ... (imports e resposta_sucesso permanecem os mesmos) ...

def resposta_erro(detail: any, status_code: int = 400): # Mudei 'mensagem' para 'detail' para alinhar com HTTPException
    """
    Cria uma JSONResponse de erro padronizada.
    Espera que o 'detail' seja o conteúdo principal do erro.
    Se 'detail' for uma string, será envolvido em um dicionário: {"detail": "sua string"}.
    Se 'detail' já for um dicionário (como o de HTTPException), será usado diretamente.
    """
    # FastAPI usa 'detail' como chave padrão para mensagens de erro em HTTPException
    # Para consistência, vamos garantir que nosso 'content' seja sempre um dict com essa chave.
    if isinstance(detail, str):
        content_body = {"detail": detail}
    elif isinstance(detail, dict): # Se já for um dict (ex: vindo de HTTPException com multiplos erros)
        content_body = detail # Usa o dict diretamente
    else:
        content_body = {"detail": str(detail)} # Fallback para outros tipos

    return JSONResponse(
        status_code=status_code,
        content=content_body
    )

# O decorator tratar_erros continua o mesmo, pois ele já passa http_exc.detail
# para resposta_erro. Se http_exc.detail for um dict, a nova resposta_erro o usará.
# Se for uma string, a nova resposta_erro o envolverá em {"detail": ...}.

# ... (tratar_erros permanece o mesmo) ...
def tratar_erros(func):
    if inspect.iscoroutinefunction(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except HTTPException as http_exc:
                return resposta_erro(str(http_exc.detail), status_code=http_exc.status_code)
            except Exception:
                print("Erro interno:", traceback.format_exc())
                return resposta_erro("Erro interno no servidor", status_code=500)
        return async_wrapper
    else:
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except HTTPException as http_exc:
                return resposta_erro(str(http_exc.detail), status_code=http_exc.status_code)
            except Exception:
                print("Erro interno:", traceback.format_exc())
                return resposta_erro("Erro interno no servidor", status_code=500)
        return sync_wrapper
