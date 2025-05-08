from fastapi import APIRouter, Depends, HTTPException
from pymongo.database import Database
from config.db_config import get_mongo_db
from app.crud.mongo_crud import (
    inserir_filme,
    inserir_ator,
    inserir_elenco,
    buscar_filmes_por_genero,
    atualizar_nota_filme,
    remover_filme,
    contar_filmes_por_ano,
    media_notas_por_genero,
    buscar_filmes_avancado
)

router = APIRouter(prefix="/mongo", tags=["MongoDB"])

# ------------------ INSERÇÕES ------------------

@router.post("/filmes")
def criar_filme(filme: dict, db: Database = Depends(get_mongo_db)):
    resultado = inserir_filme(db["filmes"], filme)
    return {"id_inserido": str(resultado.inserted_id)}

@router.post("/atores")
def criar_ator(ator: dict, db: Database = Depends(get_mongo_db)):
    resultado = inserir_ator(db["atores"], ator)
    return {"id_inserido": str(resultado.inserted_id)}

@router.post("/elenco")
def criar_elenco(relacao: dict, db: Database = Depends(get_mongo_db)):
    resultado = inserir_elenco(db["elenco"], relacao)
    return {"id_inserido": str(resultado.inserted_id)}

# ------------------ CONSULTAS ------------------

@router.get("/filmes/genero/{genero}")
def filmes_por_genero(genero: str, db: Database = Depends(get_mongo_db)):
    generos = genero.split(",")
    filmes = buscar_filmes_por_genero(db["filmes"], generos)
    for filme in filmes:
        filme["_id"] = str(filme["_id"])
    return filmes

@router.get("/filmes/busca-avancada")
def busca_avancada(
    genero: str,
    ano_min: int,
    nota_min: float,
    db: Database = Depends(get_mongo_db)
):
    generos = genero.split(",")
    filmes = buscar_filmes_avancado(db["filmes"], generos, ano_min, nota_min)
    
    # Remove _id se quiser
    for filme in filmes:
        filme.pop("_id", None)  # remove o campo _id se existir
    
    return filmes

# ------------------ ATUALIZAÇÃO ------------------

@router.put("/filmes/{titulo_id}/nota")
def atualizar_nota(titulo_id: str, nova_nota: float, db: Database = Depends(get_mongo_db)):
    resultado = atualizar_nota_filme(db["filmes"], titulo_id, nova_nota)
    if resultado.matched_count == 0:
        raise HTTPException(status_code=404, detail="Filme não encontrado")
    return {"modificado": resultado.modified_count}

# ------------------ REMOÇÃO ------------------

@router.delete("/filmes/{titulo_id}")
def deletar_filme(titulo_id: str, db: Database = Depends(get_mongo_db)):
    resultado = remover_filme(db["filmes"], titulo_id)
    if resultado.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Filme não encontrado")
    return {"deletado": True}

# ------------------ AGREGAÇÃO ------------------

@router.get("/filmes/contagem/ano")
def contagem_por_ano(db: Database = Depends(get_mongo_db)):
    return contar_filmes_por_ano(db["filmes"])

@router.get("/filmes/media-nota/genero")
def media_por_genero(db: Database = Depends(get_mongo_db)):
    return media_notas_por_genero(db["filmes"])

# ------------------ STATUS ------------------

@router.get("/ping")
def ping_mongo(db: Database = Depends(get_mongo_db)):
    return {"status": "Mongo conectado", "colecoes": db.list_collection_names()}
