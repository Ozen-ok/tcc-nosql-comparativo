from fastapi import APIRouter, Depends, HTTPException
from app.crud.neo4j_crud import (
    inserir_filme,
    inserir_ator,
    inserir_elenco,
    buscar_filmes_por_genero,
    atualizar_nota_filme,
    remover_filme,
    contagem_por_ano,
    media_notas_por_genero,
    buscar_filmes_avancado
)
from config.db_config import get_neo4j_driver

router = APIRouter(prefix="/neo4j", tags=["Neo4j"])

# ------------------ INSERÇÕES ------------------

@router.post("/filmes")
def criar_filme(filme: dict):
    resultado = inserir_filme(filme)
    return {"status": "Filme inserido no Neo4j"}

@router.post("/atores")
def criar_ator(ator: dict):
    resultado = inserir_ator(ator)
    return {"status": "Ator inserido no Neo4j"}

@router.post("/elenco")
def criar_elenco(relacao: dict):
    resultado = inserir_elenco(relacao)
    return {"status": "Elenco inserido no Neo4j"}

# ------------------ CONSULTAS ------------------

@router.get("/filmes/genero/{genero}")
def filmes_por_genero(genero: str):
    generos = genero.split(",")
    filmes = buscar_filmes_por_genero(generos)
    return filmes

@router.get("/filmes/busca-avancada")
def busca_avancada(
    genero: str,
    ano_min: int,
    nota_min: float
):
    generos = genero.split(",")
    filmes = buscar_filmes_avancado(generos, ano_min, nota_min)
    return filmes

# ------------------ ATUALIZAÇÃO ------------------

@router.put("/filmes/{titulo_id}/nota")
def atualizar_nota(titulo_id: str, nova_nota: float):
    resultado = atualizar_nota_filme(titulo_id, nova_nota)
    if not resultado:
        raise HTTPException(status_code=404, detail="Filme não encontrado")
    return {"status": "Nota do filme atualizada"}

# ------------------ REMOÇÃO ------------------

@router.delete("/filmes/{titulo_id}")
def deletar_filme(titulo_id: str):
    resultado = remover_filme(titulo_id)
    if not resultado:
        raise HTTPException(status_code=404, detail="Filme não encontrado")
    return {"status": "Filme deletado"}

# ------------------ AGREGAÇÃO ------------------

@router.get("/filmes/contagem/ano")
def contar_filmes_por_ano():
    return contagem_por_ano()

@router.get("/filmes/media-nota/genero")
def media_por_genero():
    return media_notas_por_genero()

# ------------------ STATUS ------------------

@router.get("/ping")
def ping():
    return {"status": "Neo4j conectado"}
 

