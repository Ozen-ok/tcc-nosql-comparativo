from fastapi import APIRouter, Depends, HTTPException
from app.crud.redis_crud import (
    inserir_filme,
    inserir_ator,
    inserir_elenco,
    buscar_filmes_por_genero,
    buscar_filmes_avancado,
    atualizar_nota_filme,
    remover_filme,
    contar_filmes_por_ano,
    media_notas_por_genero
)
from config.db_config import get_redis_client

router = APIRouter(prefix="/redis", tags=["Redis"])
# ------------------ INSERÇÕES ------------------

@router.post("/filmes")
def criar_filme(filme: dict, r = Depends(get_redis_client)):
    inserir_filme(r, filme)
    return {"status": "Filme inserido no Redis"}

@router.post("/atores")
def criar_ator(ator: dict, r = Depends(get_redis_client)):
    inserir_ator(r, ator)
    return {"status": "Ator inserido no Redis"}

@router.post("/elenco")
def criar_elenco(relacao: dict, r = Depends(get_redis_client)):
    inserir_elenco(r, relacao)
    return {"status": "Elenco inserido no Redis"}

# ------------------ CONSULTAS ------------------

@router.get("/filmes/genero/{genero}")
def filmes_por_genero(genero: str, r = Depends(get_redis_client)):
    generos = genero.split(",")
    filmes = buscar_filmes_por_genero(r, generos)
    return filmes

@router.get("/filmes/busca-avancada")
def busca_avancada(genero: str, ano_min: int, nota_min: float, r = Depends(get_redis_client)):
    generos = genero.split(",")
    filmes = buscar_filmes_avancado(r, generos, ano_min, nota_min)
    return filmes

# ------------------ ATUALIZAÇÃO ------------------

@router.put("/filmes/{titulo_id}/nota")
def atualizar_nota(titulo_id: str, nova_nota: float, r = Depends(get_redis_client)):
    if not r.exists(f"filme:{titulo_id}"):
        raise HTTPException(status_code=404, detail="Filme não encontrado")
    atualizar_nota_filme(r, titulo_id, nova_nota)
    return {"status": "Nota do filme atualizada"}

# ------------------ REMOÇÃO ------------------

@router.delete("/filmes/{titulo_id}")
def deletar_filme(titulo_id: str, r = Depends(get_redis_client)):
    if not r.exists(f"filme:{titulo_id}"):
        raise HTTPException(status_code=404, detail="Filme não encontrado")
    remover_filme(r, titulo_id)
    return {"status": "Filme deletado"}

# ------------------ AGREGAÇÃO ------------------

@router.get("/filmes/contagem/ano")
def contar_filmes(r = Depends(get_redis_client)):
    return contar_filmes_por_ano(r)

@router.get("/filmes/media-nota/genero")
def media_por_genero(r = Depends(get_redis_client)):
    return media_notas_por_genero(r)

# ------------------ STATUS ------------------

@router.get("/ping")
def ping(r = Depends(get_redis_client)):
    try:
        r.ping()
        return {"status": "Redis conectado"}
    except:
        raise HTTPException(status_code=500, detail="Falha na conexão com o Redis")
