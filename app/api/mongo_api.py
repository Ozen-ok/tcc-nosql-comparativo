from fastapi import APIRouter, Depends, HTTPException
from pymongo.database import Database
from config.db_config import get_mongo_db
from app.crud.mongo_crud import (
    inserir_filme,
    buscar_filmes_por_genero,
    atualizar_nota_filme,
    remover_filme,
    contar_filmes_por_ano,
    media_notas_por_genero,
    buscar_filmes_avancado
)
from typing import List

router = APIRouter(prefix="/mongo", tags=["MongoDB"])

@router.post("/filmes")
def criar_filme(filme: dict, db: Database = Depends(get_mongo_db)):
    resultado = inserir_filme(db["movies"], filme)
    return {"id_inserido": str(resultado.inserted_id)}

@router.get("/filmes/genero/{genero}")
def filmes_por_genero(genero: str, db: Database = Depends(get_mongo_db)):
    """
    Retorna filmes com o gênero especificado.
    """
    filmes_cursor = buscar_filmes_por_genero(db["movies"], genero.split(","))
    filmes = []
    for filme in filmes_cursor:
        filme['_id'] = str(filme['_id'])  # Converte ObjectId para string
        filmes.append(filme)
    return filmes



@router.put("/filmes/{titulo_id}/nota")
def atualizar_nota(titulo_id: str, nova_nota: float, db: Database = Depends(get_mongo_db)):
    resultado = atualizar_nota_filme(db["movies"], titulo_id, nova_nota)
    if resultado.matched_count == 0:
        raise HTTPException(status_code=404, detail="Filme não encontrado")
    return {"modificado": resultado.modified_count}

@router.delete("/filmes/{titulo_id}")
def deletar_filme(titulo_id: str, db: Database = Depends(get_mongo_db)):
    resultado = remover_filme(db["movies"], titulo_id)
    if resultado.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Filme não encontrado")
    return {"deletado": True}

@router.get("/filmes/contagem/ano")
def contagem_por_ano(db: Database = Depends(get_mongo_db)):
    return contar_filmes_por_ano(db["movies"])

@router.get("/filmes/media-nota/genero")
def media_por_genero(db: Database = Depends(get_mongo_db)):
    return media_notas_por_genero(db["movies"])

@router.get("/filmes/busca-avancada")
def busca_avancada(
    genero: str,  # Gêneros recebidos como string separada por vírgula
    ano_min: int,
    nota_min: float,
    db: Database = Depends(get_mongo_db)
):
    # Divide a string de gêneros em uma lista
    generos = genero.split(",")

    # Faz a busca no banco
    filmes_cursor = buscar_filmes_avancado(db["movies"], generos, ano_min, nota_min)

    # Converte ObjectId para string
    filmes = []
    for filme in filmes_cursor:
        filme['_id'] = str(filme['_id'])
        filmes.append(filme)

    return filmes


@router.get("/ping")
def ping_mongo():
    db = get_mongo_db()
    return {"status": "Mongo conectado", "colecoes": db.list_collection_names()}
