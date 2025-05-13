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


def resposta_erro(mensagem: str, status_code: int = 400):
    return JSONResponse(
        status_code=status_code,
        content=mensagem
    )

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
