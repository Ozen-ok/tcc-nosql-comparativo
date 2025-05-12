from fastapi import APIRouter, Depends
from app.utils.responses import tratar_erros, resposta_sucesso
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

router = APIRouter(prefix="/neo4j", tags=["Neo4j"])

# ------------------ INSERÇÕES ------------------

@router.post("/filmes")
@tratar_erros
def criar_filme(filme: dict):
    id_inserido = inserir_filme(filme)
    return resposta_sucesso("Filme inserido no Neo4j", {"id": id_inserido})

@router.post("/atores")
@tratar_erros
def criar_ator(ator: dict):
    id_inserido = inserir_ator(ator)
    return resposta_sucesso("Ator inserido no Neo4j", {"id": id_inserido})

@router.post("/elenco")
@tratar_erros
def criar_elenco(relacao: dict):
    inserir_elenco(relacao)
    return resposta_sucesso("Elenco inserido no Neo4j")

# ------------------ CONSULTAS ------------------

@router.get("/filmes/genero/{genero}")
@tratar_erros
def filmes_por_genero(genero: str):
    generos = genero.split(",")
    filmes = buscar_filmes_por_genero(generos)
    return resposta_sucesso("Filmes encontrados por gênero", {"filmes": filmes})

@router.get("/filmes/busca-avancada")
@tratar_erros
def busca_avancada(genero: str, ano_min: int, nota_min: float):
    generos = genero.split(",")
    filmes = buscar_filmes_avancado(generos, ano_min, nota_min)
    return resposta_sucesso("Busca avançada de filmes realizada", {"filmes": filmes})

# ------------------ ATUALIZAÇÃO ------------------

@router.put("/filmes/{titulo_id}/nota")
@tratar_erros
def atualizar_nota(titulo_id: str, nova_nota: float):
    atualizar_nota_filme(titulo_id, nova_nota)
    return resposta_sucesso("Nota do filme atualizada")

# ------------------ REMOÇÃO ------------------

@router.delete("/filmes/{titulo_id}")
@tratar_erros
def deletar_filme(titulo_id: str):
    remover_filme(titulo_id)
    return resposta_sucesso("Filme removido com sucesso")


# ------------------ AGREGAÇÃO ------------------

@router.get("/filmes/contagem/ano")
@tratar_erros
def contar_filmes_por_ano():
    resultado = contagem_por_ano()
    return resposta_sucesso("Contagem de filmes por ano realizada", {"contagem_por_ano": resultado})

@router.get("/filmes/media-nota/genero")
@tratar_erros
def media_por_genero():
    resultado = media_notas_por_genero()
    return resposta_sucesso("Média de notas por gênero calculada", {"media_notas_por_genero": resultado})
