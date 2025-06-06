# src/services/query_service.py
from fastapi import HTTPException
from typing import Dict, Any, List, Optional, Union # Adicionado Union
import traceback

# --- Importações dos CRUDs e Conexões ---
from src.databases.mongo.crud import (
    buscar_filmes_avancado as mongo_buscar_filmes_avancado,
    carregar_dados_mongo,
    inserir_filme as mongo_inserir_filme,
    buscar_filme_por_id as mongo_buscar_filme_por_id,
    buscar_atores_por_filmes as mongo_buscar_atores_por_filmes,
    atualizar_campo_filme as mongo_atualizar_campo_filme,
    remover_filme as mongo_remover_filme,
    buscar_filmes_por_ator as mongo_buscar_filmes_por_ator,
    contar_filmes_por_ano as mongo_contar_filmes_por_ano,
    media_notas_por_genero as mongo_media_notas_por_genero
)
from src.databases.mongo.connection import get_mongo_db

from src.databases.cassandra.crud import (
    buscar_filmes_avancado as cassandra_buscar_filmes_avancado,
    carregar_dados as cassandra_carregar_dados,
    inserir_filme as cassandra_inserir_filme,
    buscar_filme_por_id as cassandra_buscar_filme_por_id,
    buscar_atores_por_filmes as cassandra_buscar_atores_por_filmes,
    atualizar_campo_filme as cassandra_atualizar_campo_filme,
    remover_filme as cassandra_remover_filme,
    buscar_filmes_por_ator as cassandra_buscar_filmes_por_ator,
    contar_filmes_por_ano as cassandra_contar_filmes_por_ano,
    media_notas_por_genero as cassandra_media_notas_por_genero
)
from src.databases.cassandra.connection import get_cassandra_session

from src.databases.neo4j.crud import (
    inserir_filme as neo4j_inserir_filme,
    buscar_filme_por_id as neo4j_buscar_filme_por_id,
    buscar_atores_por_filmes as neo4j_buscar_atores_por_filmes,
    atualizar_campo_filme as neo4j_atualizar_campo_filme,
    remover_filme as neo4j_remover_filme,
    buscar_filmes_por_ator as neo4j_buscar_filmes_por_ator,
    contagem_por_ano as neo4j_contagem_por_ano,
    media_notas_por_genero as neo4j_media_notas_por_genero,
    buscar_filmes_avancado as neo4j_buscar_filmes_avancado,
    carregar_dados_neo4j
)
from src.databases.neo4j.connection import get_neo4j_driver

from src.databases.redis.crud import (
    inserir_filme as redis_inserir_filme,
    buscar_filme_por_id as redis_buscar_filme_por_id,
    buscar_atores_por_filmes as redis_buscar_atores_por_filmes,
    atualizar_campo_filme as redis_atualizar_campo_filme,
    remover_filme as redis_remover_filme,
    buscar_filmes_por_ator as redis_buscar_filmes_por_ator,
    contagem_por_ano as redis_contagem_por_ano,
    media_notas_por_genero as redis_media_notas_por_genero,
    buscar_filmes_avancado as redis_buscar_filmes_avancado,
    carregar_dados_redis
)
from src.databases.redis.connection import get_redis_client

from src.models.api_models import FiltrosBuscaAvancadaPayload, FilmePayload, CarregarBasePayload, AtualizarFilmePayload
from src.core.exceptions import ItemNotFoundError, ItemAlreadyExistsError, DataValidationError, DatabaseInteractionError

ANO_CORTE_FILMES_FUTUROS = 2025

# --- FUNÇÕES "GERAIS" (já lidam com "todos") ---
# src/services/query_service.py
# ... (imports e outras funções no início do arquivo) ...

async def servico_geral_busca_avancada_filmes(
    filtros: FiltrosBuscaAvancadaPayload, banco_alvo: str
) -> Union[List[Dict[str, Any]], Dict[str, Any]]: # Tipo de retorno ajustado
    resultados_por_banco: Dict[str, Any] = {}
    payload_filtros_dict = filtros.model_dump(exclude_none=True) 

    async def executar_busca_avancada_especifica(
        nome_banco: str, 
        funcao_crud_busca_avancada: callable, 
        obter_conexao_ou_referencia_db: callable 
    ):
        # ... (lógica interna da sub-função, como estava, para obter dados_retornados_pelo_crud)
        # A lógica para preparar filtros_crud_limpos e chamar o CRUD específico permanece
        # No final da sub-função, ela popula resultados_por_banco:
        # resultados_por_banco[nome_banco] = {"data": dados_retornados_pelo_crud, "message": ...}
        # Esta parte interna continua igual, pois o tratamento de erro por banco no modo "todos" é útil.
        try:
            dados_retornados_pelo_crud = None
            filtros_comuns_para_crud = {
                "titulo": payload_filtros_dict.get("titulo"),
                "tipo": payload_filtros_dict.get("tipo"),
                "ano_min": payload_filtros_dict.get("ano_lancamento_min"),
                "generos": payload_filtros_dict.get("generos") or [],
                "nota_min": payload_filtros_dict.get("nota_min"),
                "duracao_min": payload_filtros_dict.get("duracao_min"),
                "ordenar_por": payload_filtros_dict.get("ordenar_por", "nota"),
                "ordem": payload_filtros_dict.get("ordem", -1),
                "limite": payload_filtros_dict.get("limite", 100),
                "ano_corte_futuro": ANO_CORTE_FILMES_FUTUROS
            }
            filtros_crud_limpos = {k: v for k, v in filtros_comuns_para_crud.items() if v is not None}
            if "generos" in filtros_comuns_para_crud: filtros_crud_limpos["generos"] = filtros_comuns_para_crud["generos"] # Garante que lista vazia passe, se CRUD tratar
            filtros_crud_limpos["ordenar_por"] = filtros_comuns_para_crud["ordenar_por"]
            filtros_crud_limpos["ordem"] = filtros_comuns_para_crud["ordem"]
            filtros_crud_limpos["limite"] = filtros_comuns_para_crud["limite"]
            filtros_crud_limpos["ano_corte_futuro"] = filtros_comuns_para_crud["ano_corte_futuro"]

            if nome_banco == "mongo":
                db_mongo = obter_conexao_ou_referencia_db()
                if db_mongo is None: raise DatabaseInteractionError("Falha ao obter db MongoDB.")
                collection_filmes = db_mongo["filmes"]
                dados_retornados_pelo_crud = funcao_crud_busca_avancada(collection_filmes, **filtros_crud_limpos)
            elif nome_banco == "cassandra":
                session_cassandra = obter_conexao_ou_referencia_db()
                if session_cassandra is None: raise DatabaseInteractionError("Falha ao obter sessão Cassandra.")
                filtros_cql_cass = {}
                if filtros_crud_limpos.get("tipo"): filtros_cql_cass["tipo"] = filtros_crud_limpos["tipo"]
                filtros_py_cass = {
                    "titulo_contem": filtros_crud_limpos.get("titulo"), "tipo": filtros_crud_limpos.get("tipo"), 
                    "ano_lancamento_min": filtros_crud_limpos.get("ano_min"),
                    "generos_contem_todos": filtros_crud_limpos.get("generos"),
                    "nota_min": filtros_crud_limpos.get("nota_min"), "duracao_min": filtros_crud_limpos.get("duracao_min"),
                }
                filtros_py_cass_limpos = {k:v for k,v in filtros_py_cass.items() if v is not None or k == "generos_contem_todos"}
                dados_retornados_pelo_crud = funcao_crud_busca_avancada(
                    session=session_cassandra, tabela="filmes",
                    filtros_cql=filtros_cql_cass, filtros_python=filtros_py_cass_limpos,
                    ordenar_por=filtros_crud_limpos["ordenar_por"], ordem=filtros_crud_limpos["ordem"],
                    limite=filtros_crud_limpos["limite"], ano_corte_futuro_param=filtros_crud_limpos["ano_corte_futuro"]
                )
            elif nome_banco == "neo4j":
                driver_neo4j = obter_conexao_ou_referencia_db()
                if driver_neo4j is None: raise DatabaseInteractionError("Falha ao obter driver Neo4j.")
                with driver_neo4j.session(database="neo4j") as session_neo:
                    dados_retornados_pelo_crud = funcao_crud_busca_avancada(session_neo, **filtros_crud_limpos)
            elif nome_banco == "redis":
                r_client = obter_conexao_ou_referencia_db()
                if r_client is None: raise DatabaseInteractionError("Falha ao obter cliente Redis.")
                dados_retornados_pelo_crud = funcao_crud_busca_avancada(r_client, **filtros_crud_limpos)
            else:
                raise ValueError(f"Busca avançada não configurada para: {nome_banco}")
            resultados_por_banco[nome_banco] = {"data": dados_retornados_pelo_crud, "message": f"Busca avançada em {nome_banco.capitalize()} concluída."}
        except (ItemNotFoundError, DataValidationError, ValueError, DatabaseInteractionError) as e_domain:
            if banco_alvo.lower() == "todos":
                resultados_por_banco[nome_banco] = {"error": f"({nome_banco.capitalize()}) {str(e_domain)}", "data": []}
            else:
                raise HTTPException(status_code=(404 if isinstance(e_domain, ItemNotFoundError) else 400 if isinstance(e_domain, (DataValidationError, ValueError)) else 503),
                                    detail=f"({nome_banco.capitalize()}) {str(e_domain)}")
        except Exception as e_gen:
            traceback.print_exc()
            if banco_alvo.lower() == "todos":
                resultados_por_banco[nome_banco] = {"error": f"({nome_banco.capitalize()}) Falha inesperada: {str(e_gen)}", "data": []}
            else:
                raise HTTPException(status_code=500, detail=f"({nome_banco.capitalize()}) Falha inesperada: {str(e_gen)}")

    # ... (chamadas a executar_busca_avancada_especifica para cada banco) ...
    if banco_alvo.lower() == "mongo" or banco_alvo.lower() == "todos":
        await executar_busca_avancada_especifica("mongo", mongo_buscar_filmes_avancado, get_mongo_db)
    if banco_alvo.lower() == "cassandra" or banco_alvo.lower() == "todos":
        await executar_busca_avancada_especifica("cassandra", cassandra_buscar_filmes_avancado, get_cassandra_session)
    if banco_alvo.lower() == "neo4j" or banco_alvo.lower() == "todos":
        await executar_busca_avancada_especifica("neo4j", neo4j_buscar_filmes_avancado, get_neo4j_driver)
    if banco_alvo.lower() == "redis" or banco_alvo.lower() == "todos":
        await executar_busca_avancada_especifica("redis", redis_buscar_filmes_avancado, get_redis_client)


    # --- AJUSTE NO RETORNO PARA BANCO ÚNICO ---
    if banco_alvo.lower() != "todos":
        if banco_alvo.lower() not in resultados_por_banco:
             # Isso aconteceria se o nome do banco fosse inválido e não caísse nos ifs acima,
             # ou se uma exceção não HTTP fosse levantada antes de popular resultados_por_banco.
             raise HTTPException(status_code=500, detail=f"Resultado para '{banco_alvo}' não gerado na busca avançada (erro interno ou banco inválido).")
        
        resultado_especifico = resultados_por_banco[banco_alvo.lower()]
        if "error" in resultado_especifico: # Se o loop registrou um erro para este banco
             # A sub-função já deve ter levantado HTTPException se banco_alvo não for "todos"
             # Mas, por segurança, se um erro foi registrado (aconteceria se banco_alvo="todos" e um falhasse)
             # e de alguma forma estamos aqui, levantamos o erro.
             status_http_erro = 500
             if "status_code" in resultado_especifico : status_http_erro = resultado_especifico["status_code"] # Se o erro já tem um status
             raise HTTPException(status_code=status_http_erro, detail=resultado_especifico.get("message", resultado_especifico["error"]))
        
        # Retorna a lista de filmes diretamente, ou uma lista vazia se "data" não existir ou for None.
        return resultado_especifico.get("data", []) 
            
    return resultados_por_banco # Para "todos", retorna o dicionário completo

async def servico_geral_carregar_base(payload: CarregarBasePayload, banco_alvo: str) -> Dict[str, Any]:
    resultados_por_banco: Dict[str, Any] = {}
    async def executar_carga(nome_banco: str, funcao_crud: callable, obter_conexao_driver_ou_session: callable):
        try:
            conexao_ou_db = obter_conexao_driver_ou_session() # Recebe DB, Session, Driver ou Client
            if conexao_ou_db is None:
                raise DatabaseInteractionError(f"Falha ao obter conexão/referência para o banco {nome_banco}")

            # Os CRUDs de carga esperam a referência direta (db, session, driver, client)
            resultado_crud = funcao_crud(conexao_ou_db, payload.filmes_path, payload.atores_path, payload.elenco_path)
            resultados_por_banco[nome_banco] = resultado_crud 
        except (DataValidationError, ValueError) as e_val:
            if banco_alvo.lower() == "todos":
                resultados_por_banco[nome_banco] = {"error": f"({nome_banco.capitalize()}) {str(e_val)}", "status": "falha"}
            else:
                raise HTTPException(status_code=400, detail=f"({nome_banco.capitalize()}) {str(e_val)}")
        except DatabaseInteractionError as e_db:
            traceback.print_exc()
            if banco_alvo.lower() == "todos":
                resultados_por_banco[nome_banco] = {"error": f"({nome_banco.capitalize()}) Erro de DB: {str(e_db)}", "status": "falha"}
            else:
                raise HTTPException(status_code=503, detail=f"Serviço indisponível para {nome_banco.capitalize()} ao carregar dados: {str(e_db)}")
        except Exception as e_gen:
            traceback.print_exc()
            if banco_alvo.lower() == "todos":
                resultados_por_banco[nome_banco] = {"error": f"({nome_banco.capitalize()}) Falha crítica: {str(e_gen)}", "status": "falha"}
            else:
                raise HTTPException(status_code=500, detail=f"({nome_banco.capitalize()}) Falha crítica: {str(e_gen)}")

    if banco_alvo.lower() == "mongo" or banco_alvo.lower() == "todos":
        await executar_carga("mongo", carregar_dados_mongo, get_mongo_db)
    if banco_alvo.lower() == "cassandra" or banco_alvo.lower() == "todos":
        await executar_carga("cassandra", cassandra_carregar_dados, get_cassandra_session)
    if banco_alvo.lower() == "neo4j" or banco_alvo.lower() == "todos":
        await executar_carga("neo4j", carregar_dados_neo4j, get_neo4j_driver)
    if banco_alvo.lower() == "redis" or banco_alvo.lower() == "todos":
        await executar_carga("redis", carregar_dados_redis, get_redis_client)

    if not resultados_por_banco and banco_alvo.lower() != "todos":
        raise HTTPException(status_code=400, detail=f"Nenhuma operação de carga realizada para '{banco_alvo}'.")
    if banco_alvo.lower() != "todos":
        if banco_alvo.lower() not in resultados_por_banco:
             raise HTTPException(status_code=500, detail=f"Resultado da carga para '{banco_alvo}' não gerado.")
        resultado_especifico = resultados_por_banco[banco_alvo.lower()]
        if "error" in resultado_especifico:
             raise HTTPException(status_code=500, detail=resultado_especifico.get("message", resultado_especifico["error"]))
        return resultado_especifico
    return resultados_por_banco

async def servico_geral_inserir_filme(filme_payload: FilmePayload, banco_alvo: str) -> Dict[str, Any]:
    resultados_por_banco: Dict[str, Any] = {}
    dados_filme_para_crud = filme_payload.model_dump(exclude_unset=True)
    if "titulo_id" in dados_filme_para_crud:
        dados_filme_para_crud["_id"] = dados_filme_para_crud["titulo_id"]

    async def executar_insercao_filme(nome_banco: str, funcao_crud: callable, obter_conexao_ou_referencia_db: callable):
        try:
            doc_inserido = None
            if nome_banco == "mongo":
                db_mongo = obter_conexao_ou_referencia_db()
                if db_mongo is None: raise DatabaseInteractionError("Falha ao obter db MongoDB.")
                doc_inserido = funcao_crud(db_mongo["filmes"], dados_filme_para_crud)
            elif nome_banco == "cassandra":
                session_cassandra = obter_conexao_ou_referencia_db()
                if not session_cassandra: raise DatabaseInteractionError("Falha ao obter sessão Cassandra.")
                doc_inserido = funcao_crud(session_cassandra, dados_filme_para_crud)
            elif nome_banco == "neo4j":
                driver_neo4j = obter_conexao_ou_referencia_db()
                if not driver_neo4j: raise DatabaseInteractionError("Falha ao obter driver Neo4j.")
                with driver_neo4j.session(database="neo4j") as session_neo:
                    doc_inserido = funcao_crud(session_neo, dados_filme_para_crud)
            elif nome_banco == "redis":
                r_client = obter_conexao_ou_referencia_db()
                if not r_client: raise DatabaseInteractionError("Falha ao obter cliente Redis.")
                doc_inserido = funcao_crud(r_client, dados_filme_para_crud)
            else:
                raise ValueError(f"Inserção de filme não configurada para: {nome_banco}")
            resultados_por_banco[nome_banco] = {"data": doc_inserido, "message": f"Filme inserido em {nome_banco.capitalize()}."}
        except ItemAlreadyExistsError as e_iae:
            if banco_alvo.lower() == "todos":
                resultados_por_banco[nome_banco] = {"error": f"({nome_banco.capitalize()}) {str(e_iae)}", "status_code": 409, "data": None}
            else:
                raise HTTPException(status_code=409, detail=f"({nome_banco.capitalize()}) {str(e_iae)}")
        except DataValidationError as e_dv:
            if banco_alvo.lower() == "todos":
                resultados_por_banco[nome_banco] = {"error": f"({nome_banco.capitalize()}) {str(e_dv)}", "status_code": 400, "data": None}
            else:
                raise HTTPException(status_code=400, detail=f"({nome_banco.capitalize()}) {str(e_dv)}")
        except DatabaseInteractionError as e_db:
            traceback.print_exc()
            if banco_alvo.lower() == "todos":
                 resultados_por_banco[nome_banco] = {"error": f"({nome_banco.capitalize()}) Erro de DB: {str(e_db)}", "data": None}
            else:
                raise HTTPException(status_code=503, detail=f"Serviço indisponível para {nome_banco.capitalize()} ao inserir filme: {str(e_db)}")
        except Exception as e_gen:
            traceback.print_exc()
            if banco_alvo.lower() == "todos":
                resultados_por_banco[nome_banco] = {"error": f"({nome_banco.capitalize()}) Falha: {str(e_gen)}", "data": None}
            else:
                raise HTTPException(status_code=500, detail=f"({nome_banco.capitalize()}) Falha: {str(e_gen)}")

    if banco_alvo.lower() == "mongo" or banco_alvo.lower() == "todos":
        await executar_insercao_filme("mongo", mongo_inserir_filme, get_mongo_db)
    if banco_alvo.lower() == "cassandra" or banco_alvo.lower() == "todos":
        await executar_insercao_filme("cassandra", cassandra_inserir_filme, get_cassandra_session)
    if banco_alvo.lower() == "neo4j" or banco_alvo.lower() == "todos":
        await executar_insercao_filme("neo4j", neo4j_inserir_filme, get_neo4j_driver)
    if banco_alvo.lower() == "redis" or banco_alvo.lower() == "todos":
        await executar_insercao_filme("redis", redis_inserir_filme, get_redis_client)
        
    if not resultados_por_banco and banco_alvo.lower() != "todos":
         raise HTTPException(status_code=400, detail=f"Nenhuma operação de inserção realizada para '{banco_alvo}'.")
    if banco_alvo.lower() != "todos":
        if banco_alvo.lower() not in resultados_por_banco:
             raise HTTPException(status_code=500, detail=f"Resultado da inserção para '{banco_alvo}' não gerado.")
        resultado_especifico = resultados_por_banco[banco_alvo.lower()]
        if "error" in resultado_especifico:
             status_code_erro = resultado_especifico.get("status_code", 500)
             raise HTTPException(status_code=status_code_erro, detail=resultado_especifico["error"])
        return resultado_especifico
    return resultados_por_banco

# --- FUNÇÕES DE CONSULTA/OPERAÇÃO ESPECÍFICAS (NÃO "GERAIS") ---
# Estas permanecem como estão para operações em UM banco por vez,
# pois os endpoints delas no generic_router.py não têm "todos" no enum.
# Se precisarem suportar "todos", seguirão o padrão das funções de analytics abaixo.

async def servico_buscar_detalhes_filme(id_filme: str, banco_alvo: str) -> Dict[str, Any]:
    banco_processado = banco_alvo.lower()
    try:
        filme_detalhes: Optional[Dict[str, Any]] = None # Tipagem explícita
        if banco_processado == "mongo":
            db_mongo = get_mongo_db()
            if db_mongo is None: raise DatabaseInteractionError("Falha ao obter db MongoDB.")
            filme_detalhes = mongo_buscar_filme_por_id(db_mongo["filmes"], id_filme=id_filme)
        elif banco_processado == "cassandra":
            session_cassandra = get_cassandra_session()
            if session_cassandra is None: raise DatabaseInteractionError("Falha ao obter sessão Cassandra.")
            filme_detalhes = cassandra_buscar_filme_por_id(session_cassandra, titulo_id=id_filme)
        elif banco_processado == "neo4j":
            driver_neo4j = get_neo4j_driver()
            if driver_neo4j is None: raise DatabaseInteractionError("Falha ao obter driver Neo4j.")
            with driver_neo4j.session(database="neo4j") as session_neo:
                filme_detalhes = neo4j_buscar_filme_por_id(session_neo, id_filme=id_filme)
        elif banco_processado == "redis":
            r_client = get_redis_client()
            if r_client is None: raise DatabaseInteractionError("Falha ao obter cliente Redis.")
            filme_detalhes = redis_buscar_filme_por_id(r_client, id_filme=id_filme)
        else:
            raise HTTPException(status_code=400, detail=f"Banco '{banco_alvo}' não suportado para buscar detalhes.")
        
        if filme_detalhes is None: # Checagem explícita para None
            raise ItemNotFoundError(f"Filme ID '{id_filme}' não encontrado em {banco_alvo}.")
        return filme_detalhes
    except ItemNotFoundError as e: 
        raise HTTPException(status_code=404, detail=str(e))
    except DatabaseInteractionError as e_db: 
        traceback.print_exc()
        raise HTTPException(status_code=503, detail=f"DB error em {banco_alvo} ao buscar detalhes: {str(e_db)}")
    except Exception as e_gen: 
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Erro interno em {banco_alvo} ao buscar detalhes: {str(e_gen)}")

async def servico_buscar_atores_de_filme(id_filme: str, banco_alvo: str) -> List[Dict[str, Any]]:
    banco_processado = banco_alvo.lower()
    try:
        atores: List[Dict[str, Any]] = []
        if banco_processado == "mongo":
            db_mongo = get_mongo_db()
            if db_mongo is None: raise DatabaseInteractionError("Falha ao obter db MongoDB.")
            atores = mongo_buscar_atores_por_filmes(db_mongo["filmes"], db_mongo["elenco"], db_mongo["atores"], id_filme)
        elif banco_processado == "cassandra":
            session_cassandra = get_cassandra_session()
            if session_cassandra is None: raise DatabaseInteractionError("Falha ao obter sessão Cassandra.")
            atores = cassandra_buscar_atores_por_filmes(session_cassandra, id_filme=id_filme)
        elif banco_processado == "neo4j":
            driver_neo4j = get_neo4j_driver()
            if driver_neo4j is None: raise DatabaseInteractionError("Falha ao obter driver Neo4j.")
            with driver_neo4j.session(database="neo4j") as session_neo:
                atores = neo4j_buscar_atores_por_filmes(session_neo, id_filme=id_filme)
        elif banco_processado == "redis":
            r_client = get_redis_client()
            if r_client is None: raise DatabaseInteractionError("Falha ao obter cliente Redis.")
            atores = redis_buscar_atores_por_filmes(r_client, id_filme=id_filme)
        else:
            raise HTTPException(status_code=400, detail=f"Banco '{banco_alvo}' não suportado para buscar atores.")
        return atores
    except ItemNotFoundError as e: 
        raise HTTPException(status_code=404, detail=str(e)) 
    except DatabaseInteractionError as e_db: 
        traceback.print_exc()
        raise HTTPException(status_code=503, detail=f"DB error em {banco_alvo} ao buscar atores: {str(e_db)}")
    except Exception as e_gen: 
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Erro interno em {banco_alvo} ao buscar atores: {str(e_gen)}")

# src/services/query_service.py
# ... (imports e outras funções como estão) ...

async def servico_atualizar_filme(id_filme: str, update_payload: Dict[str, Any], banco_alvo: str) -> Dict[str, Any]:
    resultados_por_banco: Dict[str, Any] = {}
    campo = update_payload.get("campo")
    novo_valor = update_payload.get("novo_valor")

    if not campo:
        # Se for para um banco específico, levanta HTTP Exception direto.
        # Se for para "todos", podemos decidir se paramos tudo ou registramos erro.
        # Por ora, vamos manter o comportamento de levantar erro se o payload for inválido.
        raise HTTPException(status_code=400, detail="Campo para atualizar é obrigatório no payload.")

    def _executar_atualizacao_sincrono(nome_b_interno: str) -> Dict[str, Any]: # Retorna o filme atualizado
        filme_atualizado_dict_interno: Optional[Dict[str, Any]] = None
        if nome_b_interno == "mongo":
            db_mongo = get_mongo_db()
            if db_mongo is None: raise DatabaseInteractionError("Falha ao obter db MongoDB.")
            sucesso_update = mongo_atualizar_campo_filme(db_mongo["filmes"], id_filme, campo, novo_valor)
            if sucesso_update:
                filme_atualizado_dict_interno = mongo_buscar_filme_por_id(db_mongo["filmes"], id_filme)
        elif nome_b_interno == "cassandra":
            session_cassandra = get_cassandra_session()
            if session_cassandra is None: raise DatabaseInteractionError("Falha ao obter sessão Cassandra.")
            cassandra_atualizar_campo_filme(session_cassandra, id_filme, campo, novo_valor, tabela="filmes")
            filme_atualizado_dict_interno = cassandra_buscar_filme_por_id(session_cassandra, id_filme)
        elif nome_b_interno == "neo4j":
            driver_neo4j = get_neo4j_driver()
            if driver_neo4j is None: raise DatabaseInteractionError("Falha ao obter driver Neo4j.")
            with driver_neo4j.session(database="neo4j") as session_neo:
                filme_atualizado_dict_interno = neo4j_atualizar_campo_filme(session_neo, id_filme, campo, novo_valor)
        elif nome_b_interno == "redis":
            r_client = get_redis_client()
            if r_client is None: raise DatabaseInteractionError("Falha ao obter cliente Redis.")
            filme_atualizado_dict_interno = redis_atualizar_campo_filme(r_client, id_filme, campo, novo_valor)
        else:
            raise ValueError(f"Lógica de atualização para banco '{nome_b_interno}' não implementada.")
        
        if filme_atualizado_dict_interno is None:
             raise ItemNotFoundError(f"Filme ID '{id_filme}' não encontrado ou falha ao obter após atualização em {nome_b_interno}.")
        return filme_atualizado_dict_interno

    bancos_para_processar: List[str] = []
    if banco_alvo.lower() == "todos":
        bancos_para_processar = ["mongo", "cassandra", "neo4j", "redis"]
    elif banco_alvo.lower() in ["mongo", "cassandra", "neo4j", "redis"]:
        bancos_para_processar = [banco_alvo.lower()]
    else:
        raise HTTPException(status_code=400, detail=f"Banco '{banco_alvo}' não suportado para atualização.")

    for nome_banco_atual in bancos_para_processar:
        try:
            print(f"SERVICE DEBUG: Atualizando filme '{id_filme}' para o banco: {nome_banco_atual}")
            filme_retornado_do_crud = _executar_atualizacao_sincrono(nome_banco_atual)
            resultados_por_banco[nome_banco_atual] = {
                "data": filme_retornado_do_crud, 
                "message": f"Filme '{id_filme}' atualizado com sucesso em '{nome_banco_atual}'."
            }
        except (ItemNotFoundError, DataValidationError, DatabaseInteractionError) as e_crud:
            print(f"SERVICE ERROR (CRUD) em '{nome_banco_atual}' para atualização do filme '{id_filme}': {e_crud}")
            traceback.print_exc()
            status_code_http = 404 if isinstance(e_crud, ItemNotFoundError) else 400 if isinstance(e_crud, DataValidationError) else 503
            if banco_alvo.lower() == "todos":
                resultados_por_banco[nome_banco_atual] = {"error": str(e_crud), "status_code": status_code_http, "data": None}
            else:
                raise HTTPException(status_code=status_code_http, detail=f"({nome_banco_atual.capitalize()}) {str(e_crud)}")
        except Exception as e_geral:
            print(f"SERVICE ERROR (Geral) em '{nome_banco_atual}' para atualização do filme '{id_filme}': {e_geral}")
            traceback.print_exc()
            if banco_alvo.lower() == "todos":
                resultados_por_banco[nome_banco_atual] = {"error": f"Erro geral: {str(e_geral)}", "data": None}
            else:
                raise HTTPException(status_code=500, detail=f"({nome_banco_atual.capitalize()}) Falha inesperada: {str(e_geral)}")

    if not resultados_por_banco and banco_alvo.lower() != "todos":
        raise HTTPException(status_code=500, detail=f"Nenhuma operação de atualização concluída para '{banco_alvo}'.")

    # Se o alvo era um banco específico, o endpoint espera o formato {"data": filme_dict, "message": ...}
    # que já está em resultados_por_banco[banco_alvo.lower()] se não houve erro HTTP.
    if banco_alvo.lower() != "todos":
        if banco_alvo.lower() in resultados_por_banco:
            resultado_especifico = resultados_por_banco[banco_alvo.lower()]
            if "error" in resultado_especifico: # Se um erro foi pego e armazenado (modo todos, mas só um banco foi processado)
                raise HTTPException(status_code=resultado_especifico.get("status_code", 500), detail=resultado_especifico["error"])
            return resultado_especifico # Retorna {"data": filme_dict, "message": ...}
        else: # Não deveria chegar aqui se a lógica de bancos_para_processar estiver correta e erros HTTP são levantados
             raise HTTPException(status_code=404, detail=f"Resultado para atualização no banco '{banco_alvo}' não encontrado.")
            
    return resultados_por_banco # Para "todos", retorna o dicionário completo

# src/services/query_service.py
async def servico_remover_filme(id_filme: str, banco_alvo: str) -> Dict[str, Any]:
    resultados_por_banco: Dict[str, Any] = {}

    def _executar_remocao_sincrono(nome_b_interno: str, filme_id_remover: str) -> bool:
        # ... (lógica para chamar o CRUD específico, que retorna True ou levanta ItemNotFoundError/DatabaseInteractionError)
        # Exemplo para mongo:
        if nome_b_interno == "mongo":
            db_mongo = get_mongo_db()
            if db_mongo is None: raise DatabaseInteractionError(f"({nome_b_interno}) Falha ao obter db MongoDB.")
            return mongo_remover_filme(db_mongo["filmes"], id_filme=filme_id_remover)
        elif nome_b_interno == "cassandra":
            session_cassandra = get_cassandra_session()
            if session_cassandra is None: raise DatabaseInteractionError(f"({nome_b_interno}) Falha ao obter sessão Cassandra.")
            return cassandra_remover_filme(session_cassandra, tabela="filmes", titulo_id=filme_id_remover)
        elif nome_b_interno == "neo4j":
            driver_neo4j = get_neo4j_driver()
            if driver_neo4j is None: raise DatabaseInteractionError(f"({nome_b_interno}) Falha ao obter driver Neo4j.")
            with driver_neo4j.session(database="neo4j") as session_neo:
                return neo4j_remover_filme(session_neo, id_filme=filme_id_remover)
        elif nome_b_interno == "redis":
            r_client = get_redis_client()
            if r_client is None: raise DatabaseInteractionError(f"({nome_b_interno}) Falha ao obter cliente Redis.")
            return redis_remover_filme(r_client, id_filme=filme_id_remover)
        else:
            raise ValueError(f"Lógica de remoção não implementada para banco interno '{nome_b_interno}'.")
        # A checagem 'if not sucesso_crud:' foi removida daqui pois os CRUDs devem levantar exceção.

    bancos_para_processar: List[str] = []
    if banco_alvo.lower() == "todos":
        bancos_para_processar = ["mongo", "cassandra", "neo4j", "redis"]
    elif banco_alvo.lower() in ["mongo", "cassandra", "neo4j", "redis"]:
        bancos_para_processar = [banco_alvo.lower()]
    else:
        raise HTTPException(status_code=400, detail=f"Banco '{banco_alvo}' não suportado para remoção.")

    for nome_banco_atual in bancos_para_processar:
        try:
            print(f"SERVICE DEBUG: Removendo filme '{id_filme}' do banco: {nome_banco_atual}")
            # Se _executar_remocao_sincrono não levantar exceção, consideramos sucesso.
            # O valor de retorno booleano dela não é estritamente necessário aqui se ela sempre levanta erro em falha.
            _executar_remocao_sincrono(nome_banco_atual, id_filme) 
            
            # ---- ESTA É A PARTE CRUCIAL PARA O RETORNO ----
            resultados_por_banco[nome_banco_atual] = {
                "message": f"Filme ID '{id_filme}' removido de {nome_banco_atual.capitalize()}.",
                "status": "sucesso"  # <<<< Certifique-se que "status": "sucesso" está aqui!
            }
        except ItemNotFoundError as e_infe:
            print(f"SERVICE INFO (ItemNotFound) em '{nome_banco_atual}' para remoção do filme '{id_filme}': {e_infe}")
            if banco_alvo.lower() == "todos":
                resultados_por_banco[nome_banco_atual] = {"error": str(e_infe), "status_code_interno": 404, "message": f"Filme '{id_filme}' não encontrado em {nome_banco_atual}."}
            else: 
                raise HTTPException(status_code=404, detail=f"({nome_banco_atual.capitalize()}) {str(e_infe)}")
        except (DatabaseInteractionError, ValueError) as e_db_val: 
            print(f"SERVICE ERROR (DB/Value) em '{nome_banco_atual}' para remoção do filme '{id_filme}': {e_db_val}")
            # traceback.print_exc() # Descomente para debug se necessário
            if banco_alvo.lower() == "todos":
                resultados_por_banco[nome_banco_atual] = {"error": str(e_db_val), "status_code_interno": 503, "message": f"Erro de DB/Valor em {nome_banco_atual}."}
            else:
                raise HTTPException(status_code=503, detail=f"({nome_banco_atual.capitalize()}) {str(e_db_val)}")
        except Exception as e_geral:
            print(f"SERVICE ERROR (Geral) em '{nome_banco_atual}' para remoção do filme '{id_filme}': {e_geral}")
            # traceback.print_exc() # Descomente para debug se necessário
            if banco_alvo.lower() == "todos":
                resultados_por_banco[nome_banco_atual] = {"error": f"Erro geral: {str(e_geral)}", "message": f"Erro geral em {nome_banco_atual}."}
            else:
                raise HTTPException(status_code=500, detail=f"({nome_banco_atual.capitalize()}) Falha inesperada: {str(e_geral)}")
    
    if not resultados_por_banco and banco_alvo.lower() != "todos":
        raise HTTPException(status_code=500, detail=f"Nenhuma operação de remoção foi concluída para '{banco_alvo}'.")

    if banco_alvo.lower() != "todos":
        if banco_alvo.lower() in resultados_por_banco:
            resultado_especifico = resultados_por_banco[banco_alvo.lower()]
            if "error" in resultado_especifico: 
                raise HTTPException(status_code=resultado_especifico.get("status_code_interno", 500), detail=resultado_especifico["error"])
            
            # Certifique-se que o resultado_especifico tem a chave "status" aqui.
            print(f"DEBUG SERVICE (Remover Filme - Banco Único): Retornando para endpoint: {resultado_especifico}") # DEBUG
            return resultado_especifico # Deve ser {"message": "...", "status": "sucesso"}
        else: 
             raise HTTPException(status_code=404, detail=f"Resultado para remoção no banco '{banco_alvo}' não encontrado ou erro anterior.")
            
    return resultados_por_banco


async def servico_listar_filmes_por_ator(identificador_ator: str, banco_alvo: str, ordenar_por: str, ordem: int, limite: int) -> Union[List[Dict[str, Any]], Dict[str, Any]]: # Tipo de retorno ajustado
    resultados_por_banco: Dict[str, Any] = {}

    # Sub-função síncrona para executar a busca em um banco
    def _executar_busca_sincrono(nome_b_interno: str) -> List[Dict[str, Any]]:
        filmes_lista: List[Dict[str, Any]] = []
        if nome_b_interno == "mongo":
            db_mongo = get_mongo_db()
            if db_mongo is None: raise DatabaseInteractionError(f"({nome_b_interno}) Falha ao obter db MongoDB.")
            filmes_lista = mongo_buscar_filmes_por_ator(db_mongo["filmes"], db_mongo["elenco"], db_mongo["atores"], identificador_ator, ordenar_por, ordem, limite)
        elif nome_b_interno == "cassandra":
            session_cassandra = get_cassandra_session()
            if session_cassandra is None: raise DatabaseInteractionError(f"({nome_b_interno}) Falha ao obter sessão Cassandra.")
            filmes_lista = cassandra_buscar_filmes_por_ator(session_cassandra, identificador_ator=identificador_ator, ordenar_por=ordenar_por, ordem=ordem, limite=limite)
        elif nome_b_interno == "neo4j":
            driver_neo4j = get_neo4j_driver()
            if driver_neo4j is None: raise DatabaseInteractionError(f"({nome_b_interno}) Falha ao obter driver Neo4j.")
            with driver_neo4j.session(database="neo4j") as session_neo:
                filmes_lista = neo4j_buscar_filmes_por_ator(session_neo, id_ator=identificador_ator, ordenar_por=ordenar_por, ordem=ordem, limite=limite)
        elif nome_b_interno == "redis":
            r_client = get_redis_client()
            if r_client is None: raise DatabaseInteractionError(f"({nome_b_interno}) Falha ao obter cliente Redis.")
            filmes_lista = redis_buscar_filmes_por_ator(r_client, id_ator=identificador_ator, ordenar_por=ordenar_por, ordem=ordem, limite=limite)
        else:
            raise ValueError(f"Busca de filmes por ator não implementada para banco interno '{nome_b_interno}'.")
        return filmes_lista # Retorna a lista de filmes ou [] se o ator não for encontrado (ItemNotFoundError é tratado abaixo)

    bancos_para_processar: List[str] = []
    if banco_alvo.lower() == "todos":
        bancos_para_processar = ["mongo", "cassandra", "neo4j", "redis"]
    elif banco_alvo.lower() in ["mongo", "cassandra", "neo4j", "redis"]:
        bancos_para_processar = [banco_alvo.lower()]
    else:
        raise HTTPException(status_code=400, detail=f"Banco '{banco_alvo}' não suportado para listar filmes por ator.")

    if not bancos_para_processar:
        raise HTTPException(status_code=400, detail=f"Nenhum banco válido para processar com alvo: '{banco_alvo}'.")

    for nome_banco_atual in bancos_para_processar:
        try:
            print(f"SERVICE DEBUG: Listando filmes por ator '{identificador_ator}' para o banco: {nome_banco_atual}")
            lista_filmes_do_banco = _executar_busca_sincrono(nome_banco_atual)
            resultados_por_banco[nome_banco_atual] = {
                "data": lista_filmes_do_banco, 
                "message": f"Filmes por ator para '{nome_banco_atual}' processados."
            }
        except ItemNotFoundError as e_infe: # Se o CRUD levantar ItemNotFoundError (ator não encontrado)
            print(f"SERVICE INFO (ItemNotFound) em '{nome_banco_atual}' para listar filmes do ator '{identificador_ator}': {e_infe}")
            if banco_alvo.lower() == "todos":
                resultados_por_banco[nome_banco_atual] = {"data": [], "message": f"Ator '{identificador_ator}' não encontrado ou sem filmes em {nome_banco_atual}."}
            else: # Se for um banco específico, retorna lista vazia como antes
                return [] 
        except (DatabaseInteractionError, ValueError) as e_db_val:
            print(f"SERVICE ERROR (DB/Value) em '{nome_banco_atual}' para listar filmes por ator: {e_db_val}")
            traceback.print_exc()
            if banco_alvo.lower() == "todos":
                resultados_por_banco[nome_banco_atual] = {"error": str(e_db_val), "data": []}
            else:
                raise HTTPException(status_code=503, detail=f"({nome_banco_atual.capitalize()}) {str(e_db_val)}")
        except Exception as e_geral:
            print(f"SERVICE ERROR (Geral) em '{nome_banco_atual}' para listar filmes por ator: {e_geral}")
            traceback.print_exc()
            if banco_alvo.lower() == "todos":
                resultados_por_banco[nome_banco_atual] = {"error": f"Erro geral: {str(e_geral)}", "data": []}
            else:
                raise HTTPException(status_code=500, detail=f"({nome_banco_atual.capitalize()}) Falha inesperada: {str(e_geral)}")

    if not resultados_por_banco and banco_alvo.lower() != "todos":
        raise HTTPException(status_code=500, detail=f"Nenhuma operação de listar filmes por ator foi concluída para '{banco_alvo}'.")

    if banco_alvo.lower() != "todos":
        if banco_alvo.lower() in resultados_por_banco:
            resultado_especifico = resultados_por_banco[banco_alvo.lower()]
            if "error" in resultado_especifico:
                raise HTTPException(status_code=500, detail=resultado_especifico["error"])
            return resultado_especifico.get("data", []) # Retorna a lista de filmes diretamente
        else:
             raise HTTPException(status_code=404, detail=f"Resultado para o banco '{banco_alvo}' não encontrado ao listar filmes por ator.")
            
    return resultados_por_banco # Para "todos", retorna o dicionário completo

# --- FUNÇÕES DE ANÁLISE (MODIFICADAS PARA LIDAR COM "todos") ---
async def servico_contar_filmes_por_ano(banco_alvo: str) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    resultados_por_banco: Dict[str, Any] = {}

    def _executar_contagem_sincrono(nome_b_interno: str) -> List[Dict[str, Any]]:
        if nome_b_interno == "mongo":
            db_mongo = get_mongo_db()
            if db_mongo is None: raise DatabaseInteractionError("Falha ao obter db MongoDB.")
            return mongo_contar_filmes_por_ano(db_mongo["filmes"])
        elif nome_b_interno == "cassandra":
            session_cassandra = get_cassandra_session()
            if session_cassandra is None: raise DatabaseInteractionError("Falha ao obter sessão Cassandra.")
            return cassandra_contar_filmes_por_ano(session_cassandra)
        elif nome_b_interno == "neo4j":
            driver_neo4j = get_neo4j_driver()
            if driver_neo4j is None: raise DatabaseInteractionError("Falha ao obter driver Neo4j.")
            with driver_neo4j.session(database="neo4j") as session_neo:
                return neo4j_contagem_por_ano(session_neo)
        elif nome_b_interno == "redis":
            r_client = get_redis_client()
            if r_client is None: raise DatabaseInteractionError("Falha ao obter cliente Redis.")
            return redis_contagem_por_ano(r_client)
        else:
            raise ValueError(f"Contagem por ano não implementada para banco interno '{nome_b_interno}'.")

    bancos_para_processar: List[str] = []
    if banco_alvo.lower() == "todos":
        bancos_para_processar = ["mongo", "cassandra", "neo4j", "redis"]
    elif banco_alvo.lower() in ["mongo", "cassandra", "neo4j", "redis"]:
        bancos_para_processar = [banco_alvo.lower()]
    else:
        raise HTTPException(status_code=400, detail=f"Banco '{banco_alvo}' não suportado para contagem.")

    for nome_banco_atual in bancos_para_processar:
        try:
            print(f"SERVICE DEBUG: Contando filmes por ano para o banco: {nome_banco_atual}")
            resultado_banco_especifico = _executar_contagem_sincrono(nome_banco_atual)
            resultados_por_banco[nome_banco_atual] = {
                "data": resultado_banco_especifico, 
                "message": f"Contagem de filmes por ano para '{nome_banco_atual}' processada."
            }
        except DatabaseInteractionError as e_db_interact:
            print(f"SERVICE ERROR (DB Interaction) em '{nome_banco_atual}' para contagem: {e_db_interact}")
            traceback.print_exc()
            if banco_alvo.lower() == "todos":
                resultados_por_banco[nome_banco_atual] = {"error": f"Erro de DB: {str(e_db_interact)}", "data": []}
            else:
                raise HTTPException(status_code=503, detail=f"({nome_banco_atual.capitalize()}) Erro de DB: {str(e_db_interact)}")
        except Exception as e_geral:
            print(f"SERVICE ERROR (Geral) em '{nome_banco_atual}' para contagem: {e_geral}")
            traceback.print_exc()
            if banco_alvo.lower() == "todos":
                resultados_por_banco[nome_banco_atual] = {"error": f"Erro geral: {str(e_geral)}", "data": []}
            else:
                raise HTTPException(status_code=500, detail=f"({nome_banco_atual.capitalize()}) Falha inesperada: {str(e_geral)}")
    
    if not resultados_por_banco and banco_alvo.lower() != "todos": # Adicionado if banco_alvo != "todos"
        raise HTTPException(status_code=500, detail=f"Nenhuma operação de contagem foi concluída para '{banco_alvo}'.")

    if banco_alvo.lower() != "todos":
        if banco_alvo.lower() in resultados_por_banco:
            resultado_especifico_banco = resultados_por_banco[banco_alvo.lower()]
            if "error" in resultado_especifico_banco:
                raise HTTPException(status_code=500, detail=resultado_especifico_banco["error"])
            return resultado_especifico_banco.get("data", []) 
        else:
             raise HTTPException(status_code=404, detail=f"Resultado para o banco '{banco_alvo}' não encontrado para contagem.")
            
    return resultados_por_banco

async def servico_media_notas_por_genero(banco_alvo: str) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    resultados_por_banco: Dict[str, Any] = {}

    def _executar_media_para_banco_sincrono(nome_b_interno_sync: str) -> List[Dict[str, Any]]:
        if nome_b_interno_sync == "mongo":
            db_mongo = get_mongo_db()
            if db_mongo is None: raise DatabaseInteractionError("Falha ao obter db MongoDB.")
            return mongo_media_notas_por_genero(db_mongo["filmes"])
        elif nome_b_interno_sync == "cassandra":
            session_cassandra = get_cassandra_session()
            if session_cassandra is None: raise DatabaseInteractionError("Falha ao obter sessão Cassandra.")
            return cassandra_media_notas_por_genero(session_cassandra)
        elif nome_b_interno_sync == "neo4j":
            driver_neo4j = get_neo4j_driver()
            if driver_neo4j is None: raise DatabaseInteractionError("Falha ao obter driver Neo4j.")
            with driver_neo4j.session(database="neo4j") as session_neo:
                return neo4j_media_notas_por_genero(session_neo)
        elif nome_b_interno_sync == "redis":
            r_client = get_redis_client()
            if r_client is None: raise DatabaseInteractionError("Falha ao obter cliente Redis.")
            return redis_media_notas_por_genero(r_client)
        else:
            raise ValueError(f"Lógica interna para média de notas para banco síncrono '{nome_b_interno_sync}' não implementada.")
    
    bancos_para_processar: List[str] = []
    if banco_alvo.lower() == "todos":
        bancos_para_processar = ["mongo", "cassandra", "neo4j", "redis"]
    elif banco_alvo.lower() in ["mongo", "cassandra", "neo4j", "redis"]:
        bancos_para_processar = [banco_alvo.lower()]
    else:
        raise HTTPException(status_code=400, detail=f"Banco '{banco_alvo}' não suportado para média de notas.")

    if not bancos_para_processar: 
        raise HTTPException(status_code=400, detail=f"Nenhum banco válido para processar média de notas com alvo: '{banco_alvo}'.")

    for nome_banco in bancos_para_processar:
        try:
            print(f"SERVICE DEBUG: Calculando média de notas por gênero para o banco: {nome_banco}")
            resultado_do_banco = _executar_media_para_banco_sincrono(nome_banco)
            resultados_por_banco[nome_banco] = {
                "data": resultado_do_banco, 
                "message": f"Média de notas por gênero para '{nome_banco}' processada com sucesso."
            }
        except DatabaseInteractionError as e_db_interact:
            print(f"SERVICE ERROR (DB Interaction) em '{nome_banco}' para média de notas: {e_db_interact}")
            traceback.print_exc()
            if banco_alvo.lower() == "todos":
                resultados_por_banco[nome_banco] = {"error": f"Erro de interação com DB: {str(e_db_interact)}", "data": []}
            else:
                raise HTTPException(status_code=503, detail=f"({nome_banco.capitalize()}) Erro de DB: {str(e_db_interact)}")
        except Exception as e_geral:
            print(f"SERVICE ERROR (Geral) em '{nome_banco}' para média de notas: {e_geral}")
            traceback.print_exc()
            if banco_alvo.lower() == "todos":
                resultados_por_banco[nome_banco] = {"error": f"Erro geral no processamento: {str(e_geral)}", "data": []}
            else:
                raise HTTPException(status_code=500, detail=f"({nome_banco.capitalize()}) Falha inesperada no servidor: {str(e_geral)}")
    
    if not resultados_por_banco and banco_alvo.lower() != "todos": # Adicionado if banco_alvo != "todos"
        raise HTTPException(status_code=500, detail="Nenhuma operação de média de notas foi concluída com sucesso.")

    if banco_alvo.lower() != "todos":
        if banco_alvo.lower() in resultados_por_banco:
            resultado_especifico = resultados_por_banco[banco_alvo.lower()]
            if "error" in resultado_especifico: 
                raise HTTPException(status_code=500, detail=f"Erro ao processar para {banco_alvo.lower()}: {resultado_especifico['error']}")
            return resultado_especifico.get("data", []) 
        else:
             raise HTTPException(status_code=404, detail=f"Resultado para o banco '{banco_alvo}' não encontrado para média de notas.")
            
    return resultados_por_banco