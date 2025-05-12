from fastapi import APIRouter, Depends
from pymongo.database import Database 
from config.db_config import get_mongo_db
from app.utils.responses import tratar_erros, resposta_sucesso
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
@tratar_erros
def criar_filme(filme: dict, db: Database = Depends(get_mongo_db)):
    id_inserido = inserir_filme(db["filmes"], filme)
    return resposta_sucesso("Filme inserido com sucesso", {"id": id_inserido})

@router.post("/atores")
@tratar_erros
def criar_ator(ator: dict, db: Database = Depends(get_mongo_db)):
    id_inserido = inserir_ator(db["atores"], ator)
    return resposta_sucesso("Ator inserido com sucesso", {"id": id_inserido})

@router.post("/elenco")
@tratar_erros
def criar_elenco(relacao: dict, db: Database = Depends(get_mongo_db)):
    inserir_elenco(db["elenco"], relacao)
    return resposta_sucesso("Elenco inserido com sucesso")

# ------------------ CONSULTAS ------------------

@router.get("/filmes/genero/{generos}")
@tratar_erros
def filmes_por_generos(generos: str, db: Database = Depends(get_mongo_db)):
    genero_list = [g.strip() for g in generos.split(",")]
    filmes = buscar_filmes_por_genero(db["filmes"], genero_list)
    for filme in filmes:
        filme["_id"] = str(filme["_id"])
    return resposta_sucesso("Filmes encontrados com sucesso", {"filmes": filmes})

@router.get("/filmes/busca-avancada")
@tratar_erros
def busca_avancada(genero: str, ano_min: int, nota_min: float, db: Database = Depends(get_mongo_db)):
    generos = genero.split(",")
    filmes = buscar_filmes_avancado(db["filmes"], generos, ano_min, nota_min)
    for filme in filmes:
        filme.pop("_id", None)
    return resposta_sucesso("Busca avançada concluída", {"filmes": filmes})

# ------------------ ATUALIZAÇÃO ------------------

@router.put("/filmes/{titulo_id}/nota")
@tratar_erros
def atualizar_nota(titulo_id: str, nova_nota: float, db: Database = Depends(get_mongo_db)):
    atualizar_nota_filme(db["filmes"], titulo_id, nova_nota)
    return resposta_sucesso("Nota do filme atualizada")

# ------------------ REMOÇÃO ------------------

@router.delete("/filmes/{titulo_id}")
@tratar_erros
def deletar_filme(titulo_id: str, db: Database = Depends(get_mongo_db)):
    remover_filme(db["filmes"], titulo_id)
    return resposta_sucesso("Filme removido com sucesso")

# ------------------ AGREGAÇÃO ------------------

@router.get("/filmes/contagem/ano")
@tratar_erros
def contagem_por_ano(db: Database = Depends(get_mongo_db)):
    resultado = contar_filmes_por_ano(db["filmes"])
    return resposta_sucesso("Contagem por ano realizada com sucesso", {"contagem_por_ano": resultado})

@router.get("/filmes/media-nota/genero")
@tratar_erros
def media_por_genero(db: Database = Depends(get_mongo_db)):
    resultado = media_notas_por_genero(db["filmes"])
    return resposta_sucesso("Média de notas por gênero calculada", {"media_notas_por_genero": resultado})
