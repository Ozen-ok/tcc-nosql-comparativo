from fastapi import APIRouter, Depends, HTTPException
from cassandra.cluster import Session
from config.db_config import get_cassandra_db
from app.crud.cassandra_crud import (
    inserir_filme,
    inserir_ator,
    inserir_elenco,
    buscar_filmes_por_genero,
    buscar_filmes_avancado,
    atualizar_nota_filme,
    remover_filme,
    contar_filmes_por_ano,
    media_notas_por_genero,
)

router = APIRouter(prefix="/cassandra", tags=["Cassandra"])

# INSERÇÕES -------------------------------------------------

@router.post("/filmes")
def criar_filme(filme: dict, db: Session = Depends(get_cassandra_db)):
    inserir_filme(db, filme)
    return {"mensagem": "Filme inserido com sucesso"}

@router.post("/atores")
def criar_ator(ator: dict, db: Session = Depends(get_cassandra_db)):
    inserir_ator(db, ator)
    return {"mensagem": "Ator inserido com sucesso"}

@router.post("/elenco")
def criar_elenco(relacao: dict, db: Session = Depends(get_cassandra_db)):
    inserir_elenco(db, relacao)
    return {"mensagem": "Elenco inserido com sucesso"}

# CONSULTAS -------------------------------------------------

@router.get("/filmes/genero/{generos}")
def filmes_por_generos(generos: str, db: Session = Depends(get_cassandra_db)):
    genero_list = [g.strip() for g in generos.split(",")]
    filmes = buscar_filmes_por_genero(db, genero_list)
    return filmes

@router.get("/filmes/busca-avancada")
def busca_avancada(
    genero: str,
    ano_min: int,
    nota_min: float,
    db: Session = Depends(get_cassandra_db)
):
    generos = genero.split(",")
    filmes = buscar_filmes_avancado(db, generos, ano_min, nota_min)
    return filmes

# ATUALIZAÇÃO -----------------------------------------------

@router.put("/filmes/{titulo_id}/nota")
def atualizar_nota(titulo_id: str, nova_nota: float, db: Session = Depends(get_cassandra_db)):
    try:
        atualizar_nota_filme(db, titulo_id, nova_nota)
        return {"mensagem": "Nota atualizada com sucesso"}
    except Exception as e:
        # Retorna um erro em caso de falha
        return {"error": f"Erro ao atualizar a nota: {str(e)}"}


# REMOÇÃO ---------------------------------------------------

@router.delete("/filmes/{titulo_id}")
def deletar_filme(titulo_id: str, db: Session = Depends(get_cassandra_db)):
    try:
        remover_filme(db, titulo_id)
        return {"mensagem": "Filme removido com sucesso"}
    except Exception as e:
        # Retorna um erro em caso de falha
        return {"error": f"Erro ao remover o filme: {str(e)}"}


# AGREGAÇÕES / ESTATÍSTICAS ---------------------------------

@router.get("/filmes/contagem/ano")
def contagem_por_ano(db: Session = Depends(get_cassandra_db)):
    return contar_filmes_por_ano(db)

@router.get("/filmes/media-nota/genero")
def media_por_genero(db: Session = Depends(get_cassandra_db)):
    return media_notas_por_genero(db)

# HEALTH CHECK ----------------------------------------------

@router.get("/ping")
def ping_cassandra(db: Session = Depends(get_cassandra_db)):
    return {"status": "Cassandra conectado"}
