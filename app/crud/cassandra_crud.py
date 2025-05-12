from fastapi import HTTPException

# INSERÇÕES -----------------------------------------------

def inserir_filme(session, filme: dict):
    query = """
    INSERT INTO filmes (titulo_id, titulo, tipo, ano_lancamento, generos, nota, numero_votos, duracao, sinopse)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    session.execute(query, (
        filme["titulo_id"],
        filme["titulo"],
        filme["tipo"],
        filme["ano_lancamento"],
        filme["generos"],
        filme["nota"],
        filme["numero_votos"],
        filme["duracao"],
        filme["sinopse"]
    ))

def inserir_ator(session, ator: dict):
    query = """
    INSERT INTO atores (ator_id, nome_ator, ano_nascimento)
    VALUES (%s, %s, %s)
    """
    session.execute(query, (
        ator["ator_id"],
        ator["nome_ator"],
        ator["ano_nascimento"]
    ))

def inserir_elenco(session, relacao: dict):
    query = """
    INSERT INTO elenco (ator_id, titulo_id, nome_personagem)
    VALUES (%s, %s, %s)
    """
    session.execute(query, (
        relacao["ator_id"],
        relacao["titulo_id"],
        relacao["nome_personagem"]
    ))

# CONSULTAS -----------------------------------------------

def buscar_filmes_por_genero(session, generos: list):
    query = "SELECT * FROM filmes ALLOW FILTERING"
    rows = session.execute(query)

    generos_set = set(generos)
    return [
        row._asdict()
        for row in rows
        if generos_set.issubset(set(row.generos))
    ]

def buscar_filmes_avancado(session, generos: list, ano_min: int, nota_min: float):
    query = "SELECT * FROM filmes ALLOW FILTERING"
    rows = session.execute(query)

    generos_set = set(generos)
    
    return [
        row._asdict()
        for row in rows
        if generos_set.issubset(set(row.generos))
        and row.ano_lancamento >= ano_min
        and row.nota >= nota_min
    ]


# ATUALIZAÇÃO ---------------------------------------------

def atualizar_nota_filme(session, titulo_id: str, nova_nota: float):
    # Verificar se o filme existe
    query = "SELECT * FROM filmes WHERE titulo_id = %s"
    filme_existente = session.execute(query, (titulo_id,)).one()

    if not filme_existente:
        # Caso não encontre o filme, levanta um erro HTTP 404
        raise HTTPException(status_code=404, detail=f"Filme com o título ID '{titulo_id}' não encontrado")
    
    
    # Atualizar a nota do filme
    query = """
    UPDATE filmes SET nota = %s WHERE titulo_id = %s
    """
    session.execute(query, (nova_nota, titulo_id))

# REMOÇÃO -------------------------------------------------

def remover_filme(session, titulo_id: str):
    # Verificar se o filme existe
    query = "SELECT * FROM filmes WHERE titulo_id = %s"
    filme_existente = session.execute(query, (titulo_id,)).one()

    if not filme_existente:
        # Caso não encontre o filme, levanta um erro HTTP 404
        raise HTTPException(status_code=404, detail=f"Filme com o título ID '{titulo_id}' não encontrado")
    
    # Deletar o filme
    query = "DELETE FROM filmes WHERE titulo_id = %s"
    session.execute(query, (titulo_id,))



# AGREGAÇÃO / ANÁLISE -------------------------------------

def contar_filmes_por_ano(session):
    query = "SELECT ano_lancamento FROM filmes ALLOW FILTERING"
    rows = session.execute(query)

    # Dicionário para contar os filmes por ano
    contagem = {}
    for row in rows:
        ano = row.ano_lancamento
        contagem[ano] = contagem.get(ano, 0) + 1

    # Organiza os resultados por ano (equivalente ao $sort do MongoDB)
    contagem_ordenada = {k: v for k, v in sorted(contagem.items())}
    
    # Retorna os resultados no formato desejado
    return [{"_id": ano, "quantidade": quantidade} for ano, quantidade in contagem_ordenada.items()]


def media_notas_por_genero(session):
    query = "SELECT generos, nota FROM filmes ALLOW FILTERING"
    rows = session.execute(query)

    generos_notas = {}
    for row in rows:
        for genero in row.generos:
            if genero not in generos_notas:
                generos_notas[genero] = {"soma": 0.0, "contagem": 0}
            generos_notas[genero]["soma"] += row.nota
            generos_notas[genero]["contagem"] += 1

    resultado = [
        {"genero": genero, "media_nota": dados["soma"] / dados["contagem"]}
        for genero, dados in generos_notas.items()
    ]
    
    return resultado
