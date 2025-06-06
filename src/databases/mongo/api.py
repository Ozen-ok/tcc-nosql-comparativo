from fastapi import APIRouter, Depends
from pydantic import BaseModel
from pymongo.database import Database
from src.databases.mongo.connection import get_mongo_db
from src.utils.responses import tratar_erros, resposta_sucesso
from src.databases.mongo.crud import (
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
    buscar_atores_por_filmes,
    carregar_dados_mongo
)
from typing import Union, List, Optional

router = APIRouter(prefix="/mongo", tags=["MongoDB"])

class FiltrosBuscaAvancada(BaseModel):
    titulo: Optional[str] = ""
    tipo: Optional[str] = ""
    ano_lancamento: Optional[int] = 2010
    generos: Optional[List[str]] = []
    nota: Optional[float] = 7.0
    duracao: Optional[float] = 90
    ordenar_por: Optional[str] = "nota"  # Ex: 'nota', 'ano_lancamento', 'duracao'
    ordem: Optional[int] = -1  # -1 para decrescente, 1 para crescente

class InsercaoRequest(BaseModel):
    filmes_path: str
    atores_path: str
    elenco_path: str
# ------------------ INSERÇÕES ------------------

@router.post("/carregar-base")
@tratar_erros
def inserir_dados(req: InsercaoRequest):
    carregar_dados_mongo(req.filmes_path, req.atores_path, req.elenco_path)
    return resposta_sucesso("Base inserida com sucesso.")

@router.post("/filmes")
@tratar_erros
def criar_filme(filme: dict, db: Database = Depends(get_mongo_db)):
    titulo = inserir_filme(db["filmes"], filme)
    return resposta_sucesso(f"{titulo} inserido com sucesso")

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
    valor: Union[str, float, int] = None,
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
    #print(valor)
    if campo == "generos" and valor:
        valor = [v.strip() for v in valor.split(",")]

    #print(type(valor))
    filmes = buscar_filmes_simples(db["filmes"], campo, valor, ordenar_por, ordem)
    for filme in filmes:
        filme.pop("_id", None)

    return resposta_sucesso("Busca simples concluída", {"filmes": filmes})


@router.post("/filmes/busca-avancada")
@tratar_erros
def busca_avancada(payload: FiltrosBuscaAvancada, db: Database = Depends(get_mongo_db)):
    filtros = payload.model_dump()
    #print(filtros)

    filmes = buscar_filmes_avancado(
        db["filmes"],
        filtros.get("titulo"),
        filtros.get("tipo"),
        filtros.get("ano_lancamento"),
        filtros.get("generos"),
        filtros.get("nota"),
        filtros.get("duracao"),
        filtros.get("ordenar_por"),
        filtros.get("ordem")
    )
    
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

@router.put("/filmes/{titulo_id}")
@tratar_erros
def atualizar_campo(titulo_id: str, campo: str, valor: Union[str, float, int] = None, db: Database = Depends(get_mongo_db)):

    if campo == "generos" and valor:
        valor = [v.strip() for v in valor.split(",")]

    atualizar_campo_filme(db["filmes"], titulo_id, campo, valor)
    return resposta_sucesso(f"{campo} do filme atualizado")


# ------------------ REMOÇÃO ------------------

@router.delete("/filmes/{titulo_id}")
@tratar_erros
def deletar_filme(titulo_id: str, db: Database = Depends(get_mongo_db)):
    remover_filme(db["filmes"], titulo_id)
    return resposta_sucesso("Filme removido com sucesso")

# ------------------ AGREGAÇÃO ------------------

@router.get("/filmes/contagem-por-ano")
@tratar_erros
def contagem_por_ano(db: Database = Depends(get_mongo_db)):
    resultado = contar_filmes_por_ano(db["filmes"])
    return resposta_sucesso("Contagem por ano realizada com sucesso", {"contagem_por_ano": resultado})

@router.get("/filmes/media-notas-por-genero")
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

