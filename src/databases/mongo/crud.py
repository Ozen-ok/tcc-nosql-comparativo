# src/databases/mongo/crud.py
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError, BulkWriteError
import pandas as pd
from pydantic import ValidationError
from typing import List, Dict, Any, Optional
from bson import ObjectId
import re

# Importe seus modelos Pydantic
from src.models.ator import Ator #
from src.models.filme import Filme #
from src.models.elenco import Elenco #
from src.core.exceptions import (
    DatabaseOperationError,
    ItemNotFoundError,
    ItemAlreadyExistsError,
    DataValidationError,
    DatabaseInteractionError
)

def _limpar_generos_mongo(valor_generos: Any) -> List[str]:
    if not valor_generos:
        return []
    if isinstance(valor_generos, list):
        return [str(g).strip() for g in valor_generos if str(g).strip()]
    if isinstance(valor_generos, str):
        valor_limpo = re.sub(r"[\[\]\'\"]", "", valor_generos)
        return [g.strip() for g in valor_limpo.split(",") if g.strip()]
    return []

def _preparar_documento_filme_para_mongo(filme_data_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Prepara o dicionário do filme para inserção/atualização, usando titulo_id como _id."""
    if "titulo_id" not in filme_data_dict or not filme_data_dict["titulo_id"]:
        raise DataValidationError("O campo 'titulo_id' é obrigatório para filmes.")
    
    doc = filme_data_dict.copy()
    doc["_id"] = str(doc.pop("titulo_id")) # Pega o valor de titulo_id, atribui a _id e remove a chave titulo_id
    # Opcional: manter ou remover o campo 'titulo_id' original.
    # Se mantiver, Pydantic 'Filme' continua validando 'titulo_id'.
    # Se remover e seu Pydantic 'Filme' tem 'id = Field(alias="_id")', ele mapeia.
    # Vamos manter 'titulo_id' por enquanto para menor impacto nos modelos Pydantic existentes.
    return doc

def _preparar_documento_ator_para_mongo(ator_data_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Prepara o dicionário do ator para inserção/atualização, usando ator_id como _id."""
    if "ator_id" not in ator_data_dict or not ator_data_dict["ator_id"]:
        raise DataValidationError("O campo 'ator_id' é obrigatório para atores.")
    
    doc = ator_data_dict.copy()
    doc["_id"] = str(doc.pop["ator_id"])
    return doc


def _converter_objectids_em_doc(documento: Dict[str, Any]) -> Dict[str, Any]:
    """
    Itera sobre um documento e converte todos os campos ObjectId para string.
    Foca primariamente no '_id', mas pode ser estendido.
    """
    if "_id" in documento and isinstance(documento["_id"], ObjectId):
        documento["_id"] = str(documento["_id"])
    
    # Se você souber de outros campos que podem ser ObjectId, adicione a conversão aqui.
    # Exemplo:
    # if "algum_outro_campo_id" in documento and isinstance(documento["algum_outro_campo_id"], ObjectId):
    #     documento["algum_outro_campo_id"] = str(documento["algum_outro_campo_id"])
        
    # Para campos que são listas de ObjectIds (se aplicável):
    # for key, value in documento.items():
    #     if isinstance(value, list) and value and isinstance(value[0], ObjectId):
    #         documento[key] = [str(oid) for oid in value]
            
    # Para campos que são dicionários aninhados contendo ObjectIds:
    # (requer uma abordagem recursiva se a profundidade for variável)

    return documento

import os,json

def ordenar_e_processar_resultados(
    cursor, 
    limite: Optional[int] = None, 
    salvar_em_arquivo: bool = False, # Novo parâmetro opcional
    nome_arquivo_debug: str = "debug_resultados_mongo.json" # Novo parâmetro opcional
) -> List[Dict[str, Any]]:
    """
    Processa o cursor do MongoDB, convertendo ObjectIds para string.
    Opcionalmente, salva os resultados em um arquivo JSON para debug.
    """
    resultados = []
    if limite is not None:
        cursor = cursor.limit(limite)
    
    for doc_original in cursor:
        # É crucial trabalhar com uma cópia se você for modificar o doc
        # e ainda quiser o doc original do cursor para alguma outra coisa (improvável aqui).
        # Mas para _converter_objectids_em_doc, já estamos fazendo uma cópia dentro dela.
        resultados.append(_converter_objectids_em_doc(doc_original))
    
    # O print que você tinha:
    # print("DEBUG CRUD - Resultados processados:", resultados) # Pode manter para debug no console do FastAPI
    print("antes de salvar")
    # --- LÓGICA PARA SALVAR EM ARQUIVO ---
    if salvar_em_arquivo and resultados: # Só salva se houver resultados e o flag estiver True
        try:
            # Define um caminho para salvar o arquivo (ex: na raiz do projeto ou uma pasta 'debug_output')
            # Certifique-se que o diretório existe ou crie-o.
            caminho_base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) # Vai para a raiz do projeto 'seu_projeto/'
            pasta_debug = os.path.join(caminho_base, "debug_output_crud")
            os.makedirs(pasta_debug, exist_ok=True) # Cria a pasta se não existir
            caminho_completo_arquivo = os.path.join(pasta_debug, nome_arquivo_debug)

            with open(caminho_completo_arquivo, 'w', encoding='utf-8') as f:
                json.dump(resultados, f, ensure_ascii=False, indent=4)
            print(f"DEBUG CRUD: Resultados da busca salvos em '{caminho_completo_arquivo}'")
        except Exception as e:
            print(f"DEBUG CRUD ERRO: Falha ao salvar resultados em arquivo: {e}")
    # --- FIM DA LÓGICA PARA SALVAR EM ARQUIVO ---
            
    return resultados

# Certifique-se que todas as suas funções de LEITURA no mongo/crud.py
# que retornam documentos (como buscar_filmes_simples, buscar_filmes_avancado,
# buscar_filme_por_id, etc.) usem 'ordenar_e_processar_resultados'
# ou apliquem _converter_objectids_em_doc diretamente nos resultados
# antes de retorná-los.

# Exemplo em buscar_filmes_avancado se ela não usasse ordenar_e_processar_resultados diretamente:
# def buscar_filmes_avancado(...):
#     # ... (lógica da query)
#     cursor = collection.find(query).sort([(ordenar_por, ordem)])
#     # Em vez de return list(cursor)
#     docs_brutos = list(cursor) # ou use o ordenar_e_processar_resultados
#     return [_converter_objectids_em_doc(doc) for doc in docs_brutos]

# Se suas funções de agregação como 'contar_filmes_por_ano' ou 'media_notas_por_genero'
# retornam um campo chamado '_id' que NÃO é um ObjectId (ex: _id é um ano ou um gênero),
# então a função _converter_objectids_em_doc não vai afetá-los indevidamente,
# pois ela checa 'isinstance(documento["_id"], ObjectId)'.

# --- INSERÇÕES ATUALIZADAS ---
def inserir_filme(collection: Collection, filme_data: Dict[str, Any]) -> Dict[str, Any]:
    """Insere um filme usando titulo_id como _id. Retorna o documento inserido."""
    # A função _preparar_documento_filme_para_mongo já valida 'titulo_id'
    # e o atribui a '_id'.
    documento_para_inserir = _preparar_documento_filme_para_mongo(filme_data)
    try:
        collection.insert_one(documento_para_inserir)

        return documento_para_inserir
    except DuplicateKeyError: # Este erro é do MongoDB para _id duplicado
        raise ItemAlreadyExistsError(f"Filme com _id (titulo_id) '{documento_para_inserir['_id']}' já existe.")
    except Exception as e:
        # Usar .get('_id') é mais seguro caso 'documento_para_inserir' não tenha _id por algum motivo extremo.
        id_filme = documento_para_inserir.get('_id', 'DESCONHECIDO')
        raise DatabaseInteractionError(f"Erro ao inserir filme com _id '{id_filme}': {e}")

def inserir_ator(collection: Collection, ator_data: Dict[str, Any]) -> Dict[str, Any]:
    """Insere um ator usando ator_id como _id. Retorna o documento inserido."""
    documento_para_inserir = _preparar_documento_ator_para_mongo(ator_data)
    try:
        collection.insert_one(documento_para_inserir)
        # O _id já é uma string (o ator_id), não precisa converter aqui.
        return documento_para_inserir
    except DuplicateKeyError:
        raise ItemAlreadyExistsError(f"Ator com _id (ator_id) '{documento_para_inserir['_id']}' já existe.")
    except Exception as e:
        id_ator = documento_para_inserir.get('_id', 'DESCONHECIDO')
        raise DatabaseInteractionError(f"Erro ao inserir ator com _id '{id_ator}': {e}")

def inserir_elenco(collection: Collection, relacao_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Insere uma relação de elenco.
    Retorna o documento inserido (com seu _id, que é um ObjectId, convertido para str).
    """
    # Para elenco, o _id é gerado pelo MongoDB e é um ObjectId.
    try:
        resultado = collection.insert_one(relacao_data) # relacao_data não tem _id pré-definido
        if resultado.acknowledged and resultado.inserted_id:
            # Busca o documento recém-inserido para obter todos os seus campos, incluindo o _id gerado
            doc_inserido = collection.find_one({"_id": resultado.inserted_id})
            if doc_inserido:
                # Converte o _id (ObjectId) do elenco para string antes de retornar
                return _converter_objectids_em_doc(doc_inserido)
            else:
                # Isso não deveria acontecer se o insert_one foi acknowledged com um inserted_id
                raise DatabaseInteractionError("Elenco inserido mas não pôde ser recuperado imediatamente.")
        else:
            raise DatabaseInteractionError("Falha na inserção do elenco: Operação não confirmada pelo MongoDB.")
    except DuplicateKeyError: # Isso aconteceria se você tivesse um índice único nos campos do elenco
                              # (ex: ator_id, titulo_id, nome_personagem)
        # A mensagem de erro deve refletir os campos que causam a duplicidade
        ator_id = relacao_data.get('ator_id', 'N/A')
        titulo_id = relacao_data.get('titulo_id', 'N/A') # No seu código original estava .get('_id'), o que não faz sentido para relacao_data.
        raise ItemAlreadyExistsError(f"Relação de elenco já existe para ator '{ator_id}' e filme '{titulo_id}'.")
    except Exception as e:
        raise DatabaseInteractionError(f"Erro inesperado ao inserir relação de elenco: {e}")

def buscar_filmes_simples(collection: Collection, campo: str, valor: Any, ordenar_por: str = 'nota', ordem: int = -1, limite: Optional[int] = 10000) -> List[Dict[str, Any]]:
    query = {}
    # Se o campo de busca for 'titulo_id' ou 'ator_id', e você padronizou para usar '_id':
    if campo == "id" and collection.name == "filmes": # Adaptar para 'id' se for o caso
        query["_id"] = str(valor) # Garante que a busca por _id seja com string
    elif campo == "id" and collection.name == "atores":
        query["_id"] = str(valor)
    elif campo == "titulo":
        query = {"titulo": {"$regex": valor, "$options": "i"}} if valor else {}
    # ... (outros campos como antes)
    elif campo == "nota":
        query = {"nota": {"$gte": float(valor)}}
    elif campo == "ano_lancamento":
        query = {"ano_lancamento": int(valor)}
    elif campo == "generos":
        query = {"generos": {"$all": valor}} if isinstance(valor, list) and valor else \
                {"generos": valor} if isinstance(valor, str) and valor else {}
    elif campo == "tipo":
        query = {"tipo": valor} if valor else {}
    else:
        raise ValueError(f"Campo de busca simples '{campo}' não reconhecido para MongoDB.")
    
    cursor = collection.find(query).sort([(ordenar_por, ordem)])
    return ordenar_e_processar_resultados(cursor, limite)

# src/databases/mongo/crud.py
from pymongo.collection import Collection
from typing import List, Dict, Any, Optional
# ... (outras importações, exceções, _limpar_generos_mongo, ordenar_e_processar_resultados) ...

def buscar_filmes_avancado(
    collection: Collection,
    titulo: Optional[str] = None,
    tipo: Optional[str] = None,
    ano_min: Optional[int] = None, # Este é o ano_lancamento_min do payload
    ano_max: Optional[int] = None, # Adicionar se quiser filtrar por um range de ano exato
    generos: Optional[List[str]] = None,
    nota_min: Optional[float] = None,
    duracao_min: Optional[int] = None,
    ordenar_por: str = "nota",
    ordem: int = -1,
    limite: Optional[int] = 10000,
    ano_corte_futuro: int = 2025 # Ano a partir do qual consideramos "futuro/sem avaliação"
) -> List[Dict[str, Any]]:
    
    # Filtros base que se aplicam a todos (futuros ou não)
    query_base = {}
    if titulo: query_base["titulo"] = {"$regex": titulo, "$options": "i"}
    if tipo: query_base["tipo"] = tipo
    if generos:
        if len(generos) == 1: query_base["generos"] = {"$in": generos}
        elif len(generos) > 1: query_base["generos"] = {"$all": generos}

    # Condições para filmes "normais" (antes do ano de corte OU que tenham nota/votos)
    condicao_filmes_normais = query_base.copy() # Começa com os filtros base
    if ano_min is not None:
        condicao_filmes_normais["ano_lancamento"] = {"$gte": ano_min, "$lt": ano_corte_futuro} # Até o ano anterior ao de corte
    else: # Se não há ano_min, mas queremos diferenciar dos futuros
        condicao_filmes_normais["ano_lancamento"] = {"$lt": ano_corte_futuro}

    if nota_min is not None:
        condicao_filmes_normais["nota"] = {"$gte": nota_min}
    # if numero_votos_min is not None: # Se você adicionar esse filtro
    #     condicao_filmes_normais["numero_votos"] = {"$gte": numero_votos_min}
    if duracao_min is not None and (not tipo or tipo.lower() != "jogo"):
        condicao_filmes_normais["duracao"] = {"$gte": duracao_min}

    # Condições para filmes "futuros" ou "sem avaliação"
    # (ano >= ano_corte_futuro E nota == 0 E numero_votos == 0)
    # Para estes, ignoramos nota_min, (votos_min) e duracao_min
    condicao_filmes_futuros = query_base.copy()
    condicao_filmes_futuros["ano_lancamento"] = {"$gte": ano_corte_futuro}
    condicao_filmes_futuros["nota"] = {"$in": [0, 0.0, None]} # Considera 0, 0.0 ou ausente/None como sem nota
    condicao_filmes_futuros["numero_votos"] = {"$in": [0, None]} # Considera 0 ou ausente/None como sem votos
    # Opcional: se ano_min for fornecido, filmes futuros também devem respeitá-lo
    if ano_min is not None:
        condicao_filmes_futuros["ano_lancamento"]["$gte"] = max(ano_min, ano_corte_futuro)


    # Query final combinando as duas condições com $or
    # No entanto, a lógica acima já segmenta pelo ano_lancamento de forma que um $or
    # direto entre condicao_filmes_normais e condicao_filmes_futuros pode ser complexo
    # se ano_min for usado em ambos.
    # Uma abordagem mais simples é construir a query principal e adicionar a lógica condicional para nota/votos/duração.

    # Abordagem Refinada para a Query:
    final_query_conditions = []
    
    # Condições base sempre aplicadas
    if titulo: final_query_conditions.append({"titulo": {"$regex": titulo, "$options": "i"}})
    if tipo: final_query_conditions.append({"tipo": tipo})
    if generos:
        if len(generos) == 1: final_query_conditions.append({"generos": {"$in": generos}})
        elif len(generos) > 1: final_query_conditions.append({"generos": {"$all": generos}})
    
    # Filtro de ano (se houver)
    if ano_min is not None and ano_max is not None:
        final_query_conditions.append({"ano_lancamento": {"$gte": ano_min, "$lte": ano_max}})
    elif ano_min is not None:
        final_query_conditions.append({"ano_lancamento": {"$gte": ano_min}})
    elif ano_max is not None:
        final_query_conditions.append({"ano_lancamento": {"$lte": ano_max}})

    # Lógica condicional para nota, votos e duração
    # Condição A: Filme é "antigo" (antes do ano_corte_futuro) -> aplica filtros de nota/votos/duração
    # Condição B: Filme é "futuro" (ano_corte_futuro ou depois) E tem nota/votos == 0 -> ignora filtros de nota/votos/duração
    # Condição C: Filme é "futuro" mas TEM nota/votos > 0 -> aplica filtros de nota/votos/duração
    
    filtros_performance = []
    if nota_min is not None:
        filtros_performance.append({"nota": {"$gte": nota_min}})
    # if numero_votos_min is not None:
    #     filtros_performance.append({"numero_votos": {"$gte": numero_votos_min}})
    if duracao_min is not None and (not tipo or tipo.lower() != "jogo"):
        filtros_performance.append({"duracao": {"$gte": duracao_min}})

    if filtros_performance: # Só adiciona a lógica do $or se houver filtros de performance
        condicao_performance_para_antigos = {
            "ano_lancamento": {"$lt": ano_corte_futuro},
            "$and": filtros_performance # Aplica todos os filtros de performance
        }
        condicao_performance_para_futuros_com_avaliacao = {
            "ano_lancamento": {"$gte": ano_corte_futuro},
            "$or": [ # Ou tem nota/votos > 0, ou não tem nota/votos (são None/0)
                {"nota": {"$gt": 0}},
                {"numero_votos": {"$gt": 0}} 
                # Adicione aqui outros campos que indicam que o filme "futuro" já tem alguma avaliação
            ],
            "$and": filtros_performance # Aplica filtros de performance se já tiver avaliação
        }
        condicao_futuros_sem_avaliacao_ignora_filtros_performance = {
            "ano_lancamento": {"$gte": ano_corte_futuro},
            "nota": {"$in": [0, 0.0, None]},
            "numero_votos": {"$in": [0, None]}
            # Para estes, não aplicamos os 'filtros_performance'
        }
        
        final_query_conditions.append({
            "$or": [
                condicao_performance_para_antigos,
                condicao_performance_para_futuros_com_avaliacao,
                condicao_futuros_sem_avaliacao_ignora_filtros_performance
            ]
        })
    
    query_final_mongo = {"$and": final_query_conditions} if final_query_conditions else {}
    # print("DEBUG MONGO Query:", query_final_mongo) # Para depurar a query

    cursor = collection.find(query_final_mongo).sort([(ordenar_por, ordem)])
    return ordenar_e_processar_resultados(cursor, limite)


# --- CONSULTAS ---
def buscar_filme_por_id(collection: Collection, id_filme: str) -> Optional[Dict[str, Any]]:
    """Busca um filme pelo seu _id (que é o titulo_id)."""
    documento = collection.find_one({"_id": id_filme})
    return documento # Já está com _id como string, sem ObjectId para converter aqui

def _buscar_documento_ator_por_id_ou_nome(
    atores_collection: Collection, 
    identificador_ator: str
) -> Optional[Dict[str, Any]]:
    """
    Busca um documento de ator na coleção especificada.
    Tenta primeiro buscar pelo _id. Se não encontrar, tenta buscar pelo nome_ator.
    Retorna o documento do ator ou None se não encontrado.
    """
    print(f"DEBUG: Buscando ator com identificador: '{identificador_ator}'")
    # Tentativa 1: Buscar como se fosse um _id
    # (Lembre-se que no seu setup, o _id do ator É o ator_id)
    documento = atores_collection.find_one({"_id": identificador_ator})
    if documento:
        print(f"DEBUG: Ator encontrado pelo _id: {documento}")
        return documento

    # Tentativa 2: Buscar como se fosse um nome_ator
    # Para nomes, pode ser útil uma busca case-insensitive, se os nomes no banco não tiverem um padrão rígido de caixa.
    # Exemplo case-insensitive: documento = atores_collection.find_one({"nome_ator": {"$regex": f"^{identificador_ator}$", "$options": "i"}})
    documento = atores_collection.find_one({"nome_ator": identificador_ator})
    if documento:
        print(f"DEBUG: Ator encontrado pelo nome_ator: {documento}")
        return documento
    
    print(f"DEBUG: Nenhum ator encontrado com o identificador '{identificador_ator}' (nem como _id, nem como nome_ator).")
    return None

# Sua função buscar_ator_por_id original, se ainda precisar dela em outros lugares:
def buscar_ator_por_id(collection: Collection, id_ator: str) -> Optional[Dict[str, Any]]:
    """Busca um ator pelo seu _id (que é o ator_id)."""
    documento = collection.find_one({"_id": id_ator})
    return documento

def buscar_filmes_por_ator(
    filmes_collection: Collection,
    elenco_collection: Collection,
    atores_collection: Collection,
    identificador_ator: str, # Pode ser o nome_ator (string) ou o ator_id (string)
    ordenar_por: str = 'ano_lancamento',
    ordem: int = -1,
    limite: Optional[int] = 10000
) -> List[Dict[str, Any]]:
    print(f"--- Iniciando buscar_filmes_por_ator para identificador: {identificador_ator} ---")
    print(f"Coleções recebidas: Filmes='{filmes_collection.name}', Elenco='{elenco_collection.name}', Atores='{atores_collection.name}'")

    # 1. Usar a função flexível para buscar o documento do ator
    ator_documento = _buscar_documento_ator_por_id_ou_nome(atores_collection, identificador_ator)
    
    if not ator_documento:
        # O print de "não encontrado" já acontece dentro de _buscar_documento_ator_por_id_ou_nome
        return [] # Retorna lista vazia se o ator não for encontrado nem por ID nem por nome
    
    # Se encontrou, pegamos o _id (que é o ator_id) para usar no pipeline
    ator_id_para_pipeline = ator_documento["_id"] 
    print(f"DEBUG: Ator processado: {ator_documento}. Usando ator_id '{ator_id_para_pipeline}' para o pipeline.")

    # Em src/databases/mongo/crud.py, dentro de buscar_filmes_por_ator

    # ... (código para buscar ator_documento e definir ator_id_para_pipeline) ...
    
    pipeline = [
        {"$match": {"ator_id": ator_id_para_pipeline}}, # Filtra no elenco pelo ator_id
        {"$lookup": {
            "from": filmes_collection.name,
            "localField": "titulo_id",      # Chave no elenco que referencia o _id dos filmes
            "foreignField": "_id",          # O _id da coleção filmes (que é o titulo_id)
            "as": "filme_info_array"        # Nome do array resultante do lookup
        }},
        {"$unwind": "$filme_info_array"}, # Desconstrói o array (ainda pode haver um filme por entrada de elenco)
        
        # Agora, agrupamos por _id do filme para garantir unicidade
        # e pegamos o primeiro documento do filme encontrado para cada _id.
        {"$group": {
            "_id": "$filme_info_array._id", # Agrupa pelo _id do filme (que é o titulo_id)
            "doc_filme_original": {"$first": "$filme_info_array"} # Pega o primeiro documento completo do filme
        }},
        
        # Substitui a raiz pelo documento do filme que pegamos
        {"$replaceRoot": {"newRoot": "$doc_filme_original"}},
        
        # Ordenação e limite no final
        {"$sort": {ordenar_por: ordem}},
    ]
    if limite is not None:
        pipeline.append({"$limit": limite})
    
    # print(f"DEBUG: Pipeline de agregação ATUALIZADO: {pipeline}")

    try:
        resultados_agregados_cursor = elenco_collection.aggregate(pipeline)
        resultados_agregados = list(resultados_agregados_cursor)
        print(f"DEBUG: Total de filmes ÚNICOS encontrados para o identificador '{identificador_ator}' (usando ator_id '{ator_id_para_pipeline}'): {len(resultados_agregados)}")
        # ... (seu print de amostra, se quiser)
    except Exception as e:
        print(f"ERRO DEBUG: Exceção durante a agregação: {e}")
        return []

    return resultados_agregados

# --- ATUALIZAÇÃO ---
def atualizar_campo_filme(collection: Collection, id_filme: str, campo_para_atualizar: str, novo_valor: Any) -> bool:
    """Atualiza um campo de um filme usando seu _id (titulo_id)."""

    resultado = collection.update_one(
        {"_id": id_filme},
        {"$set": {campo_para_atualizar: novo_valor}}
    )
    if resultado.matched_count == 0:
        raise ItemNotFoundError(f"Filme com _id '{id_filme}' não encontrado para atualização.")
    return resultado.modified_count > 0 or resultado.matched_count > 0 # Considera sucesso se encontrou, mesmo que valor não mude

# --- REMOÇÃO ---
def remover_filme(collection: Collection, id_filme: str) -> bool:
    """Remove um filme usando seu _id (titulo_id)."""

    resultado = collection.delete_one({"_id": id_filme})
    if resultado.deleted_count == 0:
        raise ItemNotFoundError(f"Filme com _id '{id_filme}' não encontrado para remoção.")
    return True

# --- AGREGAÇÃO / ANÁLISE ---
# (Contar filmes por ano e Média de notas por gênero não precisam de grandes mudanças,
# pois não dependem diretamente do formato do _id, mas sim dos campos de dados)

def contar_filmes_por_ano(collection: Collection) -> List[Dict[str, Any]]:
    pipeline = [
        {"$match": {"ano_lancamento": {"$ne": None}}},
        {"$group": {"_id": "$ano_lancamento", "quantidade": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    return list(collection.aggregate(pipeline)) # O _id aqui é o ano, não ObjectId

def media_notas_por_genero(collection: Collection) -> List[Dict[str, Any]]:
    pipeline = [
        {"$match": {"generos": {"$ne": None, "$not": {"$size": 0}}, "nota": {"$ne": None} }},
        {"$unwind": "$generos"},
        {"$group": {
            "_id": "$generos",
            "soma_nota": {"$sum": "$nota"},
            "contagem": {"$sum": 1}
        }},
        {"$project": {
            "genero": "$_id", # Renomeia _id para genero
            "media_nota": {"$round": [{"$divide": ["$soma_nota", "$contagem"]}, 2]},
            "_id": 0 # Remove o campo _id original do group stage
        }},
        {"$sort": {"media_nota": -1}}
    ]

    return list(collection.aggregate(pipeline))


def buscar_atores_por_filmes( # Atores de UM filme específico
    collection_filmes: Collection,
    collection_elenco: Collection,
    collection_atores: Collection,
    id_filme: str
) -> List[Dict[str, Any]]:
    # Busca no elenco os atores para o titulo_id fornecido
    # (lembre-se que no elenco, 'titulo_id' ainda referencia o _id dos filmes)
    itens_elenco = list(collection_elenco.find({"titulo_id": id_filme}))
    if not itens_elenco:
        return []

    ids_atores_no_filme = [item_elenco["ator_id"] for item_elenco in itens_elenco]
    
    # Busca os detalhes dos atores cujos _ids (que são os ator_ids) estão na lista
    documentos_atores = list(collection_atores.find({"_id": {"$in": ids_atores_no_filme}}))
    
    map_ator_info = {ator["_id"]: ator for ator in documentos_atores} # _id aqui é o ator_id

    atores_finais = []
    for item_elenco in itens_elenco:
        ator_id_ref = item_elenco["ator_id"]
        info_ator = map_ator_info.get(ator_id_ref)
        if info_ator:
            # Buscar outros filmes do ator (usando o _id do ator, que é o ator_id_ref)
            outros_filmes_do_ator = buscar_filmes_por_ator(
                filmes_collection=collection_filmes,
                elenco_collection=collection_elenco,
                atores_collection=collection_atores, # Passa a coleção de atores
                identificador_ator=ator_id_ref, # Passa o _id do ator
                limite=10000 # Exemplo de limite
            )
            # Formata a lista de outros filmes para incluir apenas alguns campos
            outros_filmes_simplificado = [
                {"id": f.get("_id"), "titulo": f.get("titulo"), "ano_lancamento": f.get("ano_lancamento")}
                for f in outros_filmes_do_ator if f.get("_id") != id_filme # Exclui o filme atual
            ]

            atores_finais.append({
                "id": info_ator.get("_id"), # Que é o ator_id
                "ator_id": info_ator.get("ator_id"), # Pode manter para consistência com Pydantic
                "nome_ator": info_ator.get("nome_ator"),
                "ano_nascimento": info_ator.get("ano_nascimento"),
                "nome_personagem": item_elenco.get("nome_personagem"),
                "outros_filmes": outros_filmes_simplificado
            })
    return atores_finais


# --- CARGA DE DADOS ---
def carregar_dados_mongo(db: Database, filmes_path: str, atores_path: str, elenco_path: str) -> Dict[str, Any]:
    filmes_collection = db['filmes']
    atores_collection = db['atores']
    elenco_collection = db['elenco']

    filmes_collection.delete_many({})
    atores_collection.delete_many({})
    elenco_collection.delete_many({})

    try:
        # Índice único para elenco (ator_id + titulo_id + nome_personagem)
        # O _id do elenco será ObjectId, mas essa combinação deve ser única.
        elenco_collection.create_index(
            [("ator_id", 1), ("titulo_id", 1), ("nome_personagem", 1)],
            name="idx_elenco_unico_relacao",
            unique=True
        )
        campos_ordenacao_filmes = ["titulo", "ano_lancamento", "nota", "numero_votos", "duracao"]
        for campo in campos_ordenacao_filmes:
            filmes_collection.create_index([(campo, 1)], name=f"idx_filme_{campo}_asc")
        atores_collection.create_index([("nome_ator", 1)], name="idx_ator_nome")
    except Exception as e:
        print(f"Aviso: Problema ao criar índices no MongoDB: {e}")

    erros_carga = []
    filmes_inseridos_count = 0
    atores_inseridos_count = 0
    elenco_inserido_count = 0

    # Inserir Filmes
    try:
        filmes_df = pd.read_csv(filmes_path, sep="\t", keep_default_na=False, na_values=['\\N', ''])
        filmes_docs = []
        for idx, row in filmes_df.iterrows():
            try:
                titulo_id_val = str(row['titulo_id'])
                filme_payload = {
                    '_id': titulo_id_val, # _id é o titulo_id
                    'titulo_id': titulo_id_val, # Mantém para o modelo Pydantic Filme
                    'titulo': str(row['titulo']) if pd.notna(row['titulo']) else None,
                    'tipo': str(row['tipo']) if pd.notna(row['tipo']) else None,
                    'ano_lancamento': int(row['ano_lancamento']) if pd.notna(row['ano_lancamento']) and row['ano_lancamento'] != '' else None,
                    'generos': _limpar_generos_mongo(row.get('generos')),
                    'nota': float(row['nota']) if pd.notna(row['nota']) and row['nota'] != '' else None,
                    'numero_votos': int(row['numero_votos']) if pd.notna(row['numero_votos']) and row['numero_votos'] != '' else None,
                    'duracao': int(row['duracao']) if pd.notna(row['duracao']) and row['duracao'] != '' else None,
                    'sinopse': str(row['sinopse']) if pd.notna(row['sinopse']) else None
                }
                _ = Filme(**{k:v for k,v in filme_payload.items() if k!='_id'}) # Valida com Pydantic (sem _id)
                filmes_docs.append(filme_payload)
            except ValidationError as e_val:
                erros_carga.append({"arq": "filmes", "ln": idx+2, "id": row.get('titulo_id'), "err": "Pydantic", "det": e_val.errors()})
            except Exception as e_gen:
                erros_carga.append({"arq": "filmes", "ln": idx+2, "id": row.get('titulo_id'), "err": "Conversao", "det": str(e_gen)})
        if filmes_docs:
            try:
                res_filmes = filmes_collection.insert_many(filmes_docs, ordered=False)
                filmes_inseridos_count = len(res_filmes.inserted_ids)
            except BulkWriteError as bwe:
                filmes_inseridos_count = bwe.details.get('nInserted', 0)
                for err in bwe.details.get('writeErrors', []): erros_carga.append({"arq":"filmes", "tipo":"BulkWrite", "cod":err.get('code'), "det":err.get('errmsg')})
    except Exception as e_read:
        erros_carga.append({"arq": "filmes", "tipo": "LeituraDF", "det": str(e_read)})

    # Inserir Atores (lógica similar)
    try:
        atores_df = pd.read_csv(atores_path, sep="\t", keep_default_na=False, na_values=['\\N', ''])
        atores_docs = []
        ids_lote_ator = set()
        for idx, row in atores_df.iterrows():
            try:
                ator_id_val = str(row['ator_id'])
                if ator_id_val in ids_lote_ator: continue
                
                ator_payload = {
                    '_id': ator_id_val, # _id é o ator_id
                    'ator_id': ator_id_val, # Mantém para Pydantic
                    'nome_ator': str(row['nome_ator']) if pd.notna(row['nome_ator']) else None,
                    'ano_nascimento': int(row['ano_nascimento']) if pd.notna(row['ano_nascimento']) and row['ano_nascimento'] != '' else None
                }
                _ = Ator(**{k:v for k,v in ator_payload.items() if k!='_id'}) # Valida
                atores_docs.append(ator_payload)
                ids_lote_ator.add(ator_id_val)
            except ValidationError as e_val:
                erros_carga.append({"arq": "atores", "ln": idx+2, "id": row.get('ator_id'), "err": "Pydantic", "det": e_val.errors()})
            except Exception as e_gen:
                erros_carga.append({"arq": "atores", "ln": idx+2, "id": row.get('ator_id'), "err": "Conversao", "det": str(e_gen)})
        if atores_docs:
            try:
                res_atores = atores_collection.insert_many(atores_docs, ordered=False)
                atores_inseridos_count = len(res_atores.inserted_ids)
            except BulkWriteError as bwe:
                atores_inseridos_count = bwe.details.get('nInserted', 0)
                for err in bwe.details.get('writeErrors', []): erros_carga.append({"arq":"atores", "tipo":"BulkWrite", "cod":err.get('code'), "det":err.get('errmsg')})
    except Exception as e_read:
        erros_carga.append({"arq": "atores", "tipo": "LeituraDF", "det": str(e_read)})

    # Inserir Elenco (_id será ObjectId gerado pelo Mongo)
    try:
        elenco_df = pd.read_csv(elenco_path, sep="\t", keep_default_na=False, na_values=['\\N', ''])
        elenco_docs = []
        for idx, row in elenco_df.iterrows():
            try:
                elenco_payload = {
                    'ator_id': str(row['ator_id']), # Referencia o _id dos atores
                    'titulo_id': str(row['titulo_id']), # Referencia o _id dos filmes
                    'nome_personagem': str(row['nome_personagem']) if pd.notna(row['nome_personagem']) else None
                }
                _ = Elenco(**elenco_payload) # Valida
                elenco_docs.append(elenco_payload)
            except ValidationError as e_val:
                erros_carga.append({"arq": "elenco", "ln": idx+2, "ids": f"A:{row.get('ator_id')},F:{row.get('titulo_id')}", "err": "Pydantic", "det": e_val.errors()})
            except Exception as e_gen:
                erros_carga.append({"arq": "elenco", "ln": idx+2, "ids": f"A:{row.get('ator_id')},F:{row.get('titulo_id')}", "err": "Conversao", "det": str(e_gen)})
        if elenco_docs:
            try:
                res_elenco = elenco_collection.insert_many(elenco_docs, ordered=False)
                elenco_inserido_count = len(res_elenco.inserted_ids)
            except BulkWriteError as bwe:
                elenco_inserido_count = bwe.details.get('nInserted', 0)
                for err in bwe.details.get('writeErrors', []): erros_carga.append({"arq":"elenco", "tipo":"BulkWrite", "cod":err.get('code'), "det":err.get('errmsg')})
    except Exception as e_read:
        erros_carga.append({"arq": "elenco", "tipo": "LeituraDF", "det": str(e_read)})

    # Retorno final
    msg = f"Carga MongoDB: {filmes_inseridos_count} filmes, {atores_inseridos_count} atores, {elenco_inserido_count} relações."
    status_op = "sucesso" if not erros_carga else "concluído_com_erros"
    if erros_carga: print(f"LOG DE CARGA MONGO - Erros: {erros_carga}")

    return {
        "status": status_op, "message": msg,
        "detalhes_carga": {"filmes": filmes_inseridos_count, "atores": atores_inseridos_count, "elenco": elenco_inserido_count},
        "erros_execucao": erros_carga
    }