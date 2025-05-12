from config.db_config import get_neo4j_driver 
from fastapi import HTTPException

driver = get_neo4j_driver()
# INSERÇÃO -------------------------------------------------

# Inserir Filme
def inserir_filme(filme):
    
    query = """
    MERGE (f:Filme {titulo_id: $titulo_id})
    SET f.titulo = $titulo,
        f.tipo = $tipo,
        f.ano_lancamento = $ano_lancamento,
        f.generos = $generos,
        f.nota = $nota,
        f.numero_votos = $numero_votos,
        f.duracao = $duracao,
        f.sinopse = $sinopse
    """
    try:
        with driver.session() as session:
            session.run(query, **filme)
    except Exception as e:
        print(f"Erro ao inserir o filme: {e}")

# Inserir Ator
def inserir_ator(ator):
    
    query = """
    MERGE (a:Ator {ator_id: $ator_id})
    SET a.nome_ator = $nome_ator,
        a.ano_nascimento = $ano_nascimento
    """
    try:
        with driver.session() as session:
            session.run(query, **ator)
    except Exception as e:
        print(f"Erro ao inserir o ator: {e}")

# Inserir Elenco (Relação com nome_personagem)
def inserir_elenco(elenco):
     
    query = """
    MATCH (a:Ator {ator_id: $ator_id})
    MATCH (f:Filme {titulo_id: $titulo_id})
    MERGE (a)-[r:ATUOU_EM]->(f)
    SET r.nome_personagem = $nome_personagem
    """
    try:
        with driver.session() as session:
            session.run(query, **elenco)
    except Exception as e:
        print(f"Erro ao inserir o elenco: {e}")

# CONSULTAS -----------------------------------------------

def buscar_filmes_por_genero(generos: list):
     
    query = """
    MATCH (f:Filme)
    WHERE ALL(g IN $generos WHERE g IN f.generos)
    RETURN f
    """
    try:
        with driver.session() as session:
            result = session.run(query, generos=generos)
            return [record["f"] for record in result]
    except Exception as e:
        print(f"Erro ao buscar filmes por gênero: {e}")
        return []

def buscar_filmes_avancado(generos: list, ano_min: int, nota_min: float):
    
    query = """
    MATCH (f:Filme)
    WHERE ALL(g IN $generos WHERE g IN f.generos)
      AND f.ano_lancamento >= $ano_min
      AND f.nota >= $nota_min
    RETURN f
    """
    try:
        with driver.session() as session:
            result = session.run(query, generos=generos, ano_min=ano_min, nota_min=nota_min)
            return [record["f"] for record in result]
    except Exception as e:
        print(f"Erro na busca avançada: {e}")
        return []

# ATUALIZAÇÃO ---------------------------------------------

def atualizar_nota_filme(titulo_id: str, nova_nota: float):
    # Primeiro, verificar se o filme existe
    query_check = """
    MATCH (f:Filme {titulo_id: $titulo_id})
    RETURN f
    """
    
    query_update = """
    MATCH (f:Filme {titulo_id: $titulo_id})
    SET f.nota = $nova_nota
    RETURN f
    """
    
    session = driver.session() 
    # Verificar se o filme existe
    result_check = session.run(query_check, titulo_id=titulo_id)
    
    if not result_check.single():
        raise HTTPException(status_code=404, detail=f"Filme com o título ID '{titulo_id}' não encontrado")

    # Se o filme existe, atualize a nota
    result_update = session.run(query_update, titulo_id=titulo_id, nova_nota=nova_nota)
    return result_update.single()["f"] if result_update.peek() else None



# REMOÇÃO -------------------------------------------------

def remover_filme(titulo_id: str) -> bool:
    query_check = """
    MATCH (f:Filme {titulo_id: $titulo_id})
    RETURN f
    """
    query_delete = """
    MATCH (f:Filme {titulo_id: $titulo_id})
    DETACH DELETE f
    """
    
    session = driver.session() 
    # Verificar se o filme existe
    result = session.run(query_check, titulo_id=titulo_id)
    if result.single() is None:  # Se o filme não for encontrado
        raise HTTPException(status_code=404, detail=f"Filme com o título ID '{titulo_id}' não encontrado")
    
    # Se o filme existe, deletar o filme
    session.run(query_delete, titulo_id=titulo_id)
    return True  # Filme removido com sucesso

# AGREGAÇÃO / ANÁLISE -------------------------------------

def contagem_por_ano():
    query = """
    MATCH (f:Filme)
    RETURN f.ano_lancamento AS _id, count(*) AS quantidade
    ORDER BY _id
    """
    try:
        with driver.session() as session:
            result = session.run(query)
            return [{"_id": record["_id"], "quantidade": record["quantidade"]} for record in result]
    except Exception as e:
        print(f"Erro ao contar filmes por ano: {e}")
        return []


def media_notas_por_genero():
     
    query = """
    MATCH (f:Filme)
    WHERE f.nota IS NOT NULL AND f.generos IS NOT NULL
    UNWIND f.generos AS genero
    RETURN genero, avg(f.nota) AS media_nota
    ORDER BY media_nota DESC
    """
    try:
        with driver.session() as session:
            result = session.run(query)
            return [{"genero": record["genero"], "media_nota": record["media_nota"]} for record in result]
    except Exception as e:
        print(f"Erro ao calcular média de notas por gênero: {e}")
        return []

