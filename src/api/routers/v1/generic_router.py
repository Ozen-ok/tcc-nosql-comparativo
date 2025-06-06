# src/api/routers/v1/generic_router.py
from fastapi import APIRouter, Query, HTTPException, Path, Body
from typing import List, Optional, Dict, Any, Union # Adicionado Union

from src.models.api_models import (
    FiltrosBuscaAvancadaPayload, FilmePayload, CarregarBasePayload,
    AtualizarFilmePayload,
    FilmeResponse, AtorResponse, ContagemPorAnoResponse, MediaGeneroResponse, OperacaoStatusResponse
)
from src.services.query_service import (
    servico_geral_busca_avancada_filmes,
    servico_geral_carregar_base,
    servico_geral_inserir_filme,
    servico_buscar_detalhes_filme,
    servico_buscar_atores_de_filme,
    servico_atualizar_filme,
    servico_remover_filme,
    servico_listar_filmes_por_ator,
    servico_contar_filmes_por_ano,
    servico_media_notas_por_genero
)
from src.utils.responses import tratar_erros, resposta_sucesso

router = APIRouter(tags=["Operações Genéricas v1"])

# --- Endpoints "Gerais" (já lidam com "todos" e retornam Dict[str, Any]) ---
# src/api/routers/v1/generic_router.py
# ... (imports, certifique-se que Union está em typing) ...

@router.post("/filmes/busca-avancada", 
            response_model=Union[List[FilmeResponse], Dict[str, Any]], # AJUSTADO para Union
            tags=["Busca Avançada"]) # Melhor tag
@tratar_erros
async def endpoint_busca_avancada_filmes_generico(
    filtros: FiltrosBuscaAvancadaPayload,
    banco: str = Query("todos", enum=["mongo", "cassandra", "neo4j", "redis", "todos"])
):
    # O serviço servico_geral_busca_avancada_filmes agora retorna:
    # - List[Dict[str, Any]] (lista de filmes) se banco_alvo != "todos"
    # - Dict[str, Any] (com resultados por banco) se banco_alvo == "todos"
    resultados_servico = await servico_geral_busca_avancada_filmes(filtros=filtros, banco_alvo=banco)
    
    if banco.lower() != "todos":
        # Para banco único, o serviço retorna a lista de dicionários de filmes diretamente.
        # Precisamos converter para List[FilmeResponse].
        if not isinstance(resultados_servico, list):
            # Se o serviço não retornou uma lista como esperado (ex: um erro não pego que virou dict)
            print(f"ERRO ROUTER (Busca Avançada Banco Único): Esperava lista do serviço, recebeu {type(resultados_servico)}")
            if isinstance(resultados_servico, dict) and "error" in resultados_servico: # Checa se é um dict de erro
                raise HTTPException(status_code=resultados_servico.get("status_code_interno", 500), detail=resultados_servico["error"])
            raise HTTPException(status_code=500, detail="Resposta inesperada do serviço para busca avançada em banco único.")
        
        # Retorna a lista de FilmeResponse diretamente
        return [FilmeResponse(**filme) for filme in resultados_servico]
    else:
        # Para "todos", o serviço retorna Dict[str, Dict[str, Union[List,str]]]
        # (Ex: {"mongo": {"data": [lista_filmes_mongo], "message": "..."}, ...})
        # Precisamos formatar o "data" interno de cada banco para List[FilmeResponse] (serializado)
        # e então envelopar com resposta_sucesso.
        resposta_formatada_todos: Dict[str, Any] = {}
        for nome_banco_chave, res_banco in resultados_servico.items(): # resultados_servico é o dict de bancos
            if "data" in res_banco and isinstance(res_banco["data"], list):
                resposta_formatada_todos[nome_banco_chave] = {
                    "data": [FilmeResponse(**filme).model_dump() for filme in res_banco["data"]],
                    "message": res_banco.get("message", "")
                }
            else: # Caso de erro ou estrutura inesperada para um banco específico no modo "todos"
                resposta_formatada_todos[nome_banco_chave] = res_banco 
        
        return resposta_sucesso(
            mensagem=f"Busca avançada para '{banco}' processada.",
            dados=resposta_formatada_todos # O payload "dados" da resposta_sucesso será o dict dos bancos
        )

# --- Endpoint de Listagem de Filmes por Ator (MODIFICADO para "todos") ---
@router.get("/atores/{id_ator}/filmes",
            response_model=Union[List[FilmeResponse], Dict[str, Any]], # <<< AJUSTE AQUI!
            tags=["Atores", "Filmes"])
@tratar_erros
async def endpoint_listar_filmes_por_ator(
    id_ator: str = Path(..., min_length=1, description="O ID (_id) do ator."),
    banco: str = Query("mongo", enum=["mongo", "cassandra", "neo4j", "redis", "todos"]),
    ordenar_por: Optional[str] = Query("nota", description="Campo para ordenação dos filmes."),
    ordem: Optional[int] = Query(-1, description="Ordem: 1 para ASC, -1 para DESC."),
    limite: Optional[int] = Query(100, ge=1, le=1000)
):
    # O serviço servico_listar_filmes_por_ator retorna:
    # - List[Dict[str, Any]] se banco != "todos"
    # - Dict[str, Any] (com resultados por banco) se banco == "todos"
    resultado_servico = await servico_listar_filmes_por_ator(
        identificador_ator=id_ator, 
        banco_alvo=banco, 
        ordenar_por=ordenar_por, 
        ordem=ordem, 
        limite=limite
    )

    if banco.lower() != "todos":
        # Para banco único, o serviço retorna a lista de dicionários de filmes.
        # Convertemos para List[FilmeResponse] como antes.
        # Isso vai bater com a parte List[FilmeResponse] do Union.
        if not isinstance(resultado_servico, list): # Segurança extra
             raise HTTPException(status_code=500, detail="Resposta inesperada do serviço para listagem de filmes por ator em banco único.")
        return [FilmeResponse(**filme) for filme in resultado_servico]
    else:
        # Para "todos", o serviço retorna Dict[str, Dict[str, Union[List,str]]]
        # Ex: {"mongo": {"data": [lista_filmes_mongo], "message": "..."}, ...}
        # Precisamos formatar o "data" interno de cada banco para List[FilmeResponse]
        # e depois envelopar com resposta_sucesso, que retorna um Dict[str, Any].
        # Isso vai bater com a parte Dict[str, Any] do Union.
        resposta_formatada_todos: Dict[str, Any] = {}
        for nome_banco_chave, res_banco in resultado_servico.items(): # resultado_servico aqui é o dict de bancos
            if "data" in res_banco and isinstance(res_banco["data"], list):
                resposta_formatada_todos[nome_banco_chave] = {
                    "data": [FilmeResponse(**filme).model_dump() for filme in res_banco["data"]], # Converte para o modelo e depois para dict
                    "message": res_banco.get("message", "")
                }
            else: # Caso de erro ou estrutura inesperada para um banco específico no modo "todos"
                resposta_formatada_todos[nome_banco_chave] = res_banco 

        return resposta_sucesso(
            mensagem=f"Listagem de filmes para o ator ID '{id_ator}' em 'todos' os bancos processada.",
            dados=resposta_formatada_todos # Este é o Dict[str, Dict[str, Any]] que será parte do Dict[str, Any] final
        )

@router.post("/admin/carregar-base", response_model=Dict[str, Any])
@tratar_erros
async def endpoint_carregar_base_generico(
    payload: CarregarBasePayload,
    banco: str = Query("todos", enum=["mongo", "cassandra", "neo4j", "redis", "todos"])
):
    resultados_servico = await servico_geral_carregar_base(payload=payload, banco_alvo=banco)
    return resposta_sucesso(
        mensagem=f"Operação de carga para '{banco}' processada.",
        dados=resultados_servico
    )

@router.post("/filmes", response_model=Dict[str, Any], status_code=201)
@tratar_erros
async def endpoint_inserir_filme_generico(
    filme_payload: FilmePayload,
    banco: str = Query("todos", enum=["mongo", "cassandra", "neo4j", "redis", "todos"]) # "todos" adicionado aqui também
):
    resultados_servico = await servico_geral_inserir_filme(filme_payload=filme_payload, banco_alvo=banco)
    return resposta_sucesso(
        mensagem=f"Inserção de filme para '{banco}' processada.",
        dados=resultados_servico
    )

# --- Endpoints para operações em UM banco específico (mantêm-se como estão) ---
@router.get("/filmes/{id_filme}", response_model=FilmeResponse, tags=["Filmes"])
@tratar_erros
async def endpoint_buscar_detalhes_filme(
    id_filme: str = Path(..., min_length=1, description="O ID (_id) do filme a ser buscado."),
    banco: str = Query("mongo", enum=["mongo", "cassandra", "neo4j", "redis"]) # Não tem "todos"
):
    detalhes_filme_dict = await servico_buscar_detalhes_filme(id_filme=id_filme, banco_alvo=banco)
    return FilmeResponse(**detalhes_filme_dict)

@router.get("/filmes/{id_filme}/atores", response_model=List[AtorResponse], tags=["Filmes", "Atores"])
@tratar_erros
async def endpoint_buscar_atores_de_filme(
    id_filme: str = Path(..., min_length=1, description="O ID (_id) do filme para listar atores."),
    banco: str = Query("mongo", enum=["mongo", "cassandra", "neo4j", "redis"]) # Não tem "todos"
):
    lista_atores_dicts = await servico_buscar_atores_de_filme(id_filme=id_filme, banco_alvo=banco)
    return [AtorResponse(**ator) for ator in lista_atores_dicts]

# src/api/routers/v1/generic_router.py
# ... (imports e outros endpoints) ...

@router.put("/filmes/{id_filme}", 
            response_model=Dict[str, Any], # Alterado para Dict[str, Any] para ser flexível
            tags=["Filmes"])
@tratar_erros
async def endpoint_atualizar_filme(
    id_filme: str = Path(..., min_length=1),
    payload: AtualizarFilmePayload = Body(...),
    banco: str = Query("mongo", enum=["mongo", "cassandra", "neo4j", "redis", "todos"]) # "todos" ADICIONADO
):
    # O serviço agora retorna Dict[str, Any] para "todos", 
    # ou {"data": filme_dict, "message": ...} para banco específico.
    resultados_servico = await servico_atualizar_filme(
        id_filme=id_filme, 
        update_payload=payload.model_dump(), 
        banco_alvo=banco
    )
    
    if banco.lower() != "todos":
        # Para banco único, o serviço retorna {"data": filme_dict, "message": ...}
        # E o response_model do endpoint original era FilmeResponse.
        # Vamos manter a conversão para FilmeResponse para o caso de banco único.
        if "data" in resultados_servico and isinstance(resultados_servico["data"], dict):
            # Retorna o objeto FilmeResponse diretamente se for banco único e sucesso
            return FilmeResponse(**resultados_servico["data"]) 
        else: 
            # Se não tem "data" ou não é dict, algo deu errado no serviço que não foi HTTPException
            # ou o "error" já foi tratado pelo @tratar_erros ou pelo serviço.
            # Esta parte pode precisar de refinamento dependendo do retorno exato do serviço em erro.
            # Se o serviço sempre levanta HTTPException em erro para banco único, este else pode não ser atingido.
            # Se resultados_servico for um erro já formatado por resposta_erro, @tratar_erros pode já ter retornado.
            # Por segurança, se chegou aqui e não tem data, é um problema.
            raise HTTPException(status_code=500, detail=resultados_servico.get("message", "Falha ao obter filme atualizado após update."))
    else:
        # Para "todos", o serviço retorna Dict[str, Dict[str,Union[Dict,str,None]]]
        # Ex: {"mongo": {"data": filme_mongo, "message": msg}, "cassandra": {"error": err_msg, "data": None}}
        # Precisamos garantir que o "data" interno seja FilmeResponse onde aplicável
        resposta_formatada_todos: Dict[str, Any] = {}
        for nome_banco_chave, res_banco in resultados_servico.items():
            if "data" in res_banco and isinstance(res_banco["data"], dict):
                resposta_formatada_todos[nome_banco_chave] = {
                    "data": FilmeResponse(**res_banco["data"]).model_dump(), # Converte para o modelo e depois para dict
                    "message": res_banco.get("message", "")
                }
            else: # Preserva a estrutura de erro ou mensagem sem data
                resposta_formatada_todos[nome_banco_chave] = res_banco
        
        return resposta_sucesso(
            mensagem=f"Operação de atualização para o filme '{id_filme}' em 'todos' os bancos processada.",
            dados=resposta_formatada_todos
        )

# Você precisará aplicar uma lógica similar para:
# - endpoint_remover_filme
# - endpoint_listar_filmes_por_ator
# - (Se decidir) endpoint_buscar_detalhes_filme e endpoint_buscar_atores_de_filme

# --- Endpoint de Remoção de Filme (MODIFICADO) ---
# src/api/routers/v1/generic_router.py
# ... (imports e outros endpoints) ...

@router.delete("/filmes/{id_filme}", 
               response_model=Dict[str, Any], 
               tags=["Filmes"])
@tratar_erros
async def endpoint_remover_filme(
    id_filme: str = Path(..., min_length=1),
    banco: str = Query("mongo", enum=["mongo", "cassandra", "neo4j", "redis", "todos"])
):
    print(f"DEBUG ROUTER (Remover Filme): Recebido id_filme='{id_filme}', banco='{banco}'") # DEBUG INICIAL
    resultado_servico = await servico_remover_filme(id_filme=id_filme, banco_alvo=banco)
    
    print(f"DEBUG ROUTER (Remover Filme): Resultado do serviço para banco='{banco}': {resultado_servico}") # DEBUG CRUCIAL

    if banco.lower() != "todos":
        if isinstance(resultado_servico, dict) and \
           "message" in resultado_servico and \
           resultado_servico.get("status") == "sucesso": # Verifica se o serviço retornou o esperado
            
            operacao_status = OperacaoStatusResponse(
                status="sucesso", 
                message=resultado_servico.get("message", f"Filme {id_filme} processado.") # Fallback para message
            )
            print(f"DEBUG ROUTER (Remover Filme): Retornando OperacaoStatusResponse para banco único.") # DEBUG
            return operacao_status.model_dump()
        elif isinstance(resultado_servico, dict) and "error" in resultado_servico:
            # Se o serviço para banco único já formatou um erro (menos comum, ele deveria levantar HTTPExc)
            print(f"DEBUG ROUTER (Remover Filme): Erro retornado pelo serviço para banco único: {resultado_servico}") # DEBUG
            raise HTTPException(status_code=resultado_servico.get("status_code_interno", 500), detail=resultado_servico["error"])
        else: 
            # Se chegou aqui, o resultado_servico não é o esperado para sucesso nem um erro formatado
            print(f"ERRO DEBUG ROUTER (Remover Filme): Resposta INESPERADA do serviço para banco único: {resultado_servico}") # DEBUG
            raise HTTPException(status_code=500, detail="Resposta inesperada do serviço de remoção para banco único.")
    else:
        # Para "todos", o serviço retorna o dicionário de resultados por banco
        print(f"DEBUG ROUTER (Remover Filme): Retornando resultado para 'todos' os bancos.") # DEBUG
        return resposta_sucesso(
            mensagem=f"Tentativa de remoção do filme ID '{id_filme}' em 'todos' os bancos processada.",
            dados=resultado_servico 
        )
    
# --- Endpoints de Analytics (MODIFICADOS para "todos") ---
@router.get("/analytics/filmes/contagem-por-ano", 
            response_model=Dict[str, Any], # Alterado para Dict[str, Any]
            tags=["Analytics"])
@tratar_erros
async def endpoint_contar_filmes_por_ano(
    banco: str = Query("todos", enum=["mongo", "cassandra", "neo4j", "redis", "todos"]) # "todos" adicionado
):
    # servico_contar_filmes_por_ano agora retorna Union[List[Dict], Dict[str, Any]]
    resultado_servico = await servico_contar_filmes_por_ano(banco_alvo=banco)
    
    if banco.lower() != "todos":
        # Se for um banco específico, o serviço retorna List[Dict]
        # Precisamos converter para List[ContagemPorAnoResponse]
        # e envelopar com resposta_sucesso
        dados_formatados = [ContagemPorAnoResponse(**item).model_dump() for item in resultado_servico]
        return resposta_sucesso(
            mensagem=f"Contagem de filmes por ano para '{banco}' processada.",
            dados={"contagem_por_ano": dados_formatados} # Streamlit espera "contagem_por_ano" ou "data"
        )
    else:
        # Se for "todos", o serviço retorna Dict[str, Dict[str, Union[List, str]]]
        # Onde cada valor é {"data": [...], "message": "..."} ou {"error": ..., "data": []}
        # Precisamos formatar o "data" interno de cada banco
        resposta_formatada_para_todos: Dict[str, Any] = {}
        for nome_banco_chave, res_banco in resultado_servico.items():
            if "data" in res_banco and isinstance(res_banco["data"], list):
                resposta_formatada_para_todos[nome_banco_chave] = {
                    "data": [ContagemPorAnoResponse(**item).model_dump() for item in res_banco["data"]],
                    "message": res_banco.get("message", "")
                }
            else: # Caso de erro para um banco específico no modo "todos"
                resposta_formatada_para_todos[nome_banco_chave] = res_banco
        
        return resposta_sucesso(
            mensagem="Contagem de filmes por ano para 'todos' os bancos processada.",
            dados=resposta_formatada_para_todos # Este é o Dict[str, Dict[str, Any]]
        )

@router.get("/analytics/filmes/media-notas-por-genero", 
            response_model=Dict[str, Any], # Alterado para Dict[str, Any]
            tags=["Analytics"])
@tratar_erros
async def endpoint_media_notas_por_genero(
    banco: str = Query("todos", enum=["mongo", "cassandra", "neo4j", "redis", "todos"]) # "todos" adicionado
):
    # servico_media_notas_por_genero agora retorna Union[List[Dict], Dict[str, Any]]
    resultado_servico = await servico_media_notas_por_genero(banco_alvo=banco)

    if banco.lower() != "todos":
        # Se for um banco específico, o serviço retorna List[Dict]
        dados_formatados = [MediaGeneroResponse(**item).model_dump() for item in resultado_servico]
        return resposta_sucesso(
            mensagem=f"Média de notas por gênero para '{banco}' processada.",
            dados={"media_notas_por_genero": dados_formatados} # Streamlit espera "media_notas_por_genero" ou "data"
        )
    else:
        # Se for "todos", o serviço retorna Dict[str, Dict[str, Union[List, str]]]
        resposta_formatada_para_todos: Dict[str, Any] = {}
        for nome_banco_chave, res_banco in resultado_servico.items():
            if "data" in res_banco and isinstance(res_banco["data"], list):
                resposta_formatada_para_todos[nome_banco_chave] = {
                    "data": [MediaGeneroResponse(**item).model_dump() for item in res_banco["data"]],
                    "message": res_banco.get("message", "")
                }
            else: 
                resposta_formatada_para_todos[nome_banco_chave] = res_banco
        
        return resposta_sucesso(
            mensagem="Média de notas por gênero para 'todos' os bancos processada.",
            dados=resposta_formatada_para_todos
        )