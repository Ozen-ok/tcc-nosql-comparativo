from fastapi import APIRouter, Depends
from pydantic import BaseModel
from cassandra.cluster import Session
from src.databases.cassandra.connection import get_cassandra_session
from src.utils.responses import tratar_erros, resposta_sucesso
from src.databases.cassandra.crud import (
    inserir_filme as inserir_filme_cassandra,
    inserir_ator as inserir_ator_cassandra,
    inserir_elenco as inserir_elenco_cassandra,
    buscar_filmes_simples as buscar_filmes_simples_cassandra,
    buscar_filmes_avancado as buscar_filmes_avancado_cassandra,
    buscar_filmes_por_ator as buscar_filmes_por_ator_cassandra,
    atualizar_campo_filme as atualizar_campo_filme_cassandra,
    remover_filme as remover_filme_cassandra,
    contar_filmes_por_ano as contar_filmes_por_ano_cassandra,
    media_notas_por_genero as media_notas_por_genero_cassandra,
    buscar_atores_por_filmes as buscar_atores_por_filmes_cassandra,
    carregar_dados as carregar_dados_cassandra
)

router = APIRouter(prefix="/cassandra", tags=["Cassandra"])

class InsercaoRequest(BaseModel):
    filmes_path: str
    atores_path: str
    elenco_path: str

# INSERÇÕES -------------------------------------------------------------------

@router.post("/carregar-base")
@tratar_erros
def carregar_base(req: InsercaoRequest, db: Session = Depends(get_cassandra_session)):
    carregar_dados_cassandra(db, req.filmes_path, req.atores_path, req.elenco_path)
    return resposta_sucesso("Base inserida com sucesso.")

@router.post("/filmes")
@tratar_erros
def inserir_filme(filme: dict, db: Session = Depends(get_cassandra_session)):
    titulo = inserir_filme_cassandra(db, filme)
    return resposta_sucesso(f"{titulo} inserido com sucesso")

@router.post("/atores")
@tratar_erros
def inserir_ator(ator: dict, db: Session = Depends(get_cassandra_session)):
    inserir_ator_cassandra(db, ator)
    return resposta_sucesso("Ator inserido com sucesso")

@router.post("/elenco")
@tratar_erros
def inserir_elenco(relacao: dict, db: Session = Depends(get_cassandra_session)):
    inserir_elenco_cassandra(db, relacao)
    return resposta_sucesso("Relação de elenco inserida com sucesso")

# CONSULTAS -------------------------------------------------------------------

@router.get("/filmes/busca-simples")
@tratar_erros
def buscar_filmes_simples(
    tabela: str = "filmes",
    campo: str = "titulo",
    valor: str = "",
    ordenar_por: str = "nota",
    ordem: int = -1,
    db: Session = Depends(get_cassandra_session)
):
    filmes = buscar_filmes_simples_cassandra(db, tabela, campo, valor, ordenar_por, ordem)
    return resposta_sucesso("Filmes encontrados com sucesso", {"filmes": filmes})

@router.get("/filmes/busca-avancada")
@tratar_erros
def buscar_filmes_avancado(
    tabela: str = "filmes",
    titulo: str = "",
    tipo: str = "",
    ano_min: int = 0,
    generos: str = "",
    nota_min: float = 0.0,
    duracao: int = 0,
    ordenar_por: str = "nota",
    ordem: int = -1,
    db: Session = Depends(get_cassandra_session)
):
    generos_list = generos.split(",") if generos else []
    filmes = buscar_filmes_avancado_cassandra(
        db, tabela, titulo, tipo, ano_min, generos_list,
        nota_min, duracao, ordenar_por, ordem
    )
    return resposta_sucesso("Busca avançada realizada com sucesso", {"filmes": filmes})

@router.get("/filmes/por-ator/{nome_ator}")
@tratar_erros
def buscar_filmes_por_ator(
    nome_ator: str,
    ordenar_por: str = "nota",
    ordem: int = -1,
    db: Session = Depends(get_cassandra_session)
):
    filmes = buscar_filmes_por_ator_cassandra(db, "atores", "elenco", "filmes", nome_ator, ordenar_por, ordem)
    return resposta_sucesso("Filmes por ator encontrados com sucesso", {"filmes": filmes})

# ATUALIZAÇÃO -----------------------------------------------------------------

@router.put("/filmes/{titulo_id}")
@tratar_erros
def atualizar_filme(
    titulo_id: str,
    campo: str,
    valor: str,
    db: Session = Depends(get_cassandra_session)
):
    atualizar_campo_filme_cassandra(db, "filmes", titulo_id, campo, valor)
    return resposta_sucesso("Filme atualizado com sucesso")

# REMOÇÃO ---------------------------------------------------------------------

@router.delete("/filmes/{titulo_id}")
@tratar_erros
def remover_filme(titulo_id: str, db: Session = Depends(get_cassandra_session)):
    sucesso = remover_filme_cassandra(db, "filmes", titulo_id)
    if sucesso:
        return resposta_sucesso("Filme removido com sucesso")
    else:
        return {"erro": "Filme não encontrado"}

# ANÁLISES / AGREGAÇÕES ------------------------------------------------------

@router.get("/filmes/contagem-por-ano")
@tratar_erros
def contar_filmes_por_ano(db: Session = Depends(get_cassandra_session)):
    resultado = contar_filmes_por_ano_cassandra(db)
    return resposta_sucesso("Contagem por ano realizada com sucesso", {"contagem_por_ano": resultado})

@router.get("/filmes/media-notas-por-genero")
@tratar_erros
def media_notas_por_genero(db: Session = Depends(get_cassandra_session)):
    resultado = media_notas_por_genero_cassandra(db)
    return resposta_sucesso("Média de notas por gênero calculada com sucesso", {"media_notas_por_genero": resultado})

@router.get("/filmes/{titulo_id}/atores")
@tratar_erros
def buscar_atores_por_filme(titulo_id: str, db: Session = Depends(get_cassandra_session)):
    atores = buscar_atores_por_filmes_cassandra(db, titulo_id)
    return resposta_sucesso("Atores do filme encontrados com sucesso", {"atores": atores})
