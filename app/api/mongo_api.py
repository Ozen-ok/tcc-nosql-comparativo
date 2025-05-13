from fastapi import APIRouter, Depends
from pymongo.database import Database
from config.db_config import get_mongo_db
from app.utils.responses import tratar_erros, resposta_sucesso
from app.crud.mongo_crud import (
    inserir_filme,
    inserir_ator,
    inserir_elenco,
    atualizar_campo_filme,
    remover_filme,
    contar_filmes_por_ano,
    media_notas_por_genero,
    buscar_filmes_simples,
    buscar_filmes_avancado,
    buscar_filmes_por_ator,
    buscar_atores_por_filmes
)
from typing import Any

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

@router.get("/filmes/busca-simples")
@tratar_erros
def busca_simples(
    campo: str, 
    valor: Any, 
    ordenar_por: str = 'nota', 
    ordem: int = -1, 
    db: Database = Depends(get_mongo_db)
):
    """
    Endpoint para realizar uma busca simples de filmes com base em um campo específico.

    A busca simples permite filtrar filmes por campos como 'titulo', 'nota', 'ano_lancamento', 'generos', etc.,
    e ordenar os resultados por qualquer campo desejado (como 'nota' ou 'ano_lancamento').

    - **campo**: O campo a ser filtrado (ex: 'titulo', 'nota', 'ano_lancamento').
    - **valor**: O valor que será procurado no campo especificado.
    - **ordenar_por**: O campo pelo qual os filmes serão ordenados. O valor default é 'nota'.
    - **ordem**: Direção da ordenação, onde 1 é crescente e -1 é decrescente. O valor default é -1.

    - **Resposta**: Lista de filmes que atendem ao filtro simples, ordenados conforme o campo e direção especificados.

    :param campo: O campo a ser filtrado (ex: 'titulo', 'nota', 'ano_lancamento').
    :param valor: O valor que será procurado no campo especificado.
    :param ordenar_por: Campo para ordenação dos resultados. O valor default é 'nota'.
    :param ordem: Direção da ordenação (1 para crescente, -1 para decrescente).
    :param db: Dependência do banco de dados MongoDB.
    :return: Resposta de sucesso contendo a lista de filmes que atendem ao filtro.
    """
    filmes = buscar_filmes_simples(db["filmes"], campo, valor, ordenar_por, ordem)
    for filme in filmes:
        filme.pop("_id", None)
    return resposta_sucesso("Busca simples concluída", {"filmes": filmes})


@router.get("/filmes/busca-avancada")
@tratar_erros
def busca_avancada(genero: str, ano_min: int, nota_min: float, ordenar_por: str = 'nota', ordem: int = -1, db: Database = Depends(get_mongo_db)):
    generos = genero.split(",")
    filmes = buscar_filmes_avancado(db["filmes"], generos, ano_min, nota_min, ordenar_por, ordem)
    for filme in filmes:
        filme.pop("_id", None)
    return resposta_sucesso("Busca avançada concluída", {"filmes": filmes})

@router.get("/filmes/por-ator/{nome_ator}")
@tratar_erros
def filmes_por_ator(nome_ator: str, ordenar_por: str = 'nota', ordem: int = -1, db: Database = Depends(get_mongo_db)):
    filmes = buscar_filmes_por_ator(
        db["filmes"], db["elenco"], db["atores"], nome_ator, ordenar_por, ordem
    )
    for filme in filmes:
        filme.pop("_id", None)
    return resposta_sucesso(f"Filmes encontrados para o ator {nome_ator}", {"filmes": filmes})

# ------------------ ATUALIZAÇÃO ------------------

@router.put("/filmes/{titulo_id}/{campo}")
@tratar_erros
def atualizar_campo(titulo_id: str, campo: str, novo_valor: Any, db: Database = Depends(get_mongo_db)):
    atualizar_campo_filme(db["filmes"], titulo_id, campo, novo_valor)
    return resposta_sucesso(f"{campo} do filme atualizado")

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

@router.get("/filmes/{titulo_id}/atores")
@tratar_erros
def listar_atores_por_filme(titulo_id: str, db: Database = Depends(get_mongo_db)):
    """
    Lista os atores que participaram de um filme, dado seu titulo_id.

    Para cada ator, retorna:
    - ator_id
    - nome_ator
    - nome_personagem (no contexto desse filme)
    - filmes (outros títulos que o ator participou, com os respectivos titulo_id e nome_personagem)
    """
    atores_resultado = buscar_atores_por_filmes(db["filmes"], db["elenco"], db["atores"], titulo_id)
    return resposta_sucesso(f"Atores encontrados para o filme {titulo_id}", {"atores": atores_resultado})

