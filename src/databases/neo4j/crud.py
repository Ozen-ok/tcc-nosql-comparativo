# src/databases/neo4j/crud.py
from neo4j import Driver, Session, Transaction, Record # Tipagem
import neo4j
from typing import List, Dict, Any, Optional
import pandas as pd # Para carregar_dados
from pydantic import ValidationError # Para carregar_dados

# Importe seus modelos Pydantic para validação na carga de dados
from src.models.ator import Ator
from src.models.filme import Filme
from src.models.elenco import Elenco
import re # Para limpar_generos

from src.core.exceptions import (
    DataValidationError, DatabaseInteractionError,
    ItemNotFoundError, ItemAlreadyExistsError)

def _limpar_generos_cypher(valor_generos: Any) -> List[str]: # Similar ao do Mongo
    if not valor_generos: return []
    if isinstance(valor_generos, list): return [str(g).strip() for g in valor_generos if str(g).strip()]
    if isinstance(valor_generos, str):
        valor_limpo = re.sub(r"[\[\]\'\"]", "", valor_generos)
        return [g.strip() for g in valor_limpo.split(",") if g.strip()]
    return []

# src/databases/neo4j/crud.py

def _node_to_dict(node: Record, key_name: str = "node") -> Optional[Dict[str, Any]]:
    """Converte um objeto Node do Neo4j (ou um record contendo um nó) para um dicionário."""
    #print(f"DEBUG _node_to_dict: Recebido 'node' (Record): {node}, Tipo: {type(node)}") # Log inicial
    if not node:
        print("DEBUG _node_to_dict: 'node' (Record) é None. Retornando None.")
        return None
    
    item_data = None
    # Tentativa de extração de item_data
    if isinstance(node, dict):
        #print("DEBUG _node_to_dict: 'node' é um dict.")
        item_data = node.get(key_name)
        if item_data is None and len(node.keys()) == 1: 
            item_data = node[list(node.keys())[0]] # Corrigido aqui também por segurança
    elif hasattr(node, 'properties'): # Isso seria para um objeto Node direto, não um Record
        #print("DEBUG _node_to_dict: 'node' tem 'properties' (é um Node direto?).")
        item_data = dict(node.properties)
    elif isinstance(node, Record) and node.keys(): 
        #print(f"DEBUG _node_to_dict: 'node' é um Record com chaves: {list(node.keys())}.")
        if len(list(node.keys())) == 1:
            chave_principal_record = list(node.keys())[0]
            single_item = node[chave_principal_record]
            #print(f"DEBUG _node_to_dict: single_item (de Record['{chave_principal_record}']): {single_item}, Tipo: {type(single_item)}")
            
            # --- NOVA ABORDAGEM DE EXTRAÇÃO DE item_data ---
            item_data_extraido = None # Variável temporária
            if isinstance(single_item, neo4j.graph.Node):
                #print(f"DEBUG _node_to_dict: single_item É um neo4j.graph.Node. Tentando conversão direta para dict(single_item).")
                try:
                    item_data_extraido = dict(single_item) # Tenta converter o Node diretamente para um dicionário Python
                    #print(f"DEBUG _node_to_dict: Sucesso! Resultado de dict(single_item): {item_data_extraido}")
                except Exception as e_conv:
                    print(f"DEBUG _node_to_dict: ERRO ao tentar dict(single_item): {e_conv}. Isso é inesperado.")
                    # Se a conversão direta falhar, podemos tentar listar os atributos para entender melhor o objeto
                    try:
                        #print(f"DEBUG _node_to_dict: Atributos disponíveis em single_item (via dir()): {dir(single_item)}")
                        if 'properties' in dir(single_item) and isinstance(single_item.properties, dict): # Verifica se '.properties' existe e é um dict
                            #print("DEBUG _node_to_dict: Tentando fallback para single_item.properties")
                            item_data_extraido = single_item.properties
                        else:
                            print("DEBUG _node_to_dict: Fallback para .properties não é um dict ou não existe.")
                    except Exception as e_dir:
                        print(f"DEBUG _node_to_dict: Erro ao listar atributos de single_item com dir(): {e_dir}")
            elif isinstance(single_item, dict):
                print(f"DEBUG _node_to_dict: single_item já é um dicionário.")
                item_data_extraido = single_item
            else:
                print(f"DEBUG _node_to_dict: single_item (Tipo: {type(single_item)}) não é neo4j.graph.Node nem dict. Não foi possível extrair item_data.")
            
            item_data = item_data_extraido # Atribui o resultado da tentativa à variável item_data que o resto da função usa
            # --- FIM DA NOVA ABORDAGEM ---

    # Log ANTES das modificações de _id e tipos
    #print(f"DEBUG _node_to_dict: item_data ANTES das modificações de _id/tipo: {item_data}")

    if item_data and isinstance(item_data, dict):
        #print(f"DEBUG _node_to_dict: 'item_data' é um dict, processando modificações de _id e tipos...")
        # Garante que _id (baseado em titulo_id ou ator_id) exista e os tipos corretos
        if "titulo_id" in item_data:
            item_data["_id"] = str(item_data["titulo_id"])
        elif "ator_id" in item_data: # Para nós de Ator
            item_data["_id"] = str(item_data["ator_id"])
        
        for field in ["ano_lancamento", "numero_votos", "duracao", "ano_nascimento"]:
            if field in item_data and item_data[field] is not None:
                try: item_data[field] = int(item_data[field])
                except (ValueError, TypeError): 
                    print(f"DEBUG _node_to_dict: Falha ao converter '{field}' para int (valor: {item_data[field]}). Mantendo original.")
                    pass 
        if "nota" in item_data and item_data["nota"] is not None:
            try: item_data["nota"] = float(item_data["nota"])
            except (ValueError, TypeError): 
                print(f"DEBUG _node_to_dict: Falha ao converter 'nota' para float (valor: {item_data['nota']}). Mantendo original.")
                pass
        
        # --- ADICIONE ESTAS LINHAS ANTES DO RETURN ---
        # Remove os campos de timestamp do dicionário que será retornado,
        # mas eles continuam existindo no banco de dados.
        item_data.pop("timestamp_criacao", None)
        item_data.pop("timestamp_atualizacao", None)
        # --- FIM DA ADIÇÃO ---

        #print(f"DEBUG _node_to_dict: item_data APÓS modificações, PRONTO PARA RETORNAR: {item_data}")
        return item_data
    else: 
        print(f"DEBUG _node_to_dict: Condição 'if item_data and isinstance(item_data, dict)' FALHOU.")
        print(f"DEBUG _node_to_dict: item_data final: {item_data}, tipo de item_data: {type(item_data)}. Retornando None.")
        return None


def _execute_read_query(tx: Transaction, query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    result = tx.run(query, params if params else {})
    return [_node_to_dict(record) for record in result if _node_to_dict(record) is not None]

def _execute_write_query_single_return(tx: Transaction, query: str, params: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
    #print(f"DEBUG NEO4J CRUD: Executando query: {query}") # Log 1
    #print(f"DEBUG NEO4J CRUD: Com params: {params}")     # Log 2
    result = tx.run(query, params if params else {})
    try:
        record = result.single()
        # Veja o que o Neo4j efetivamente retorna ANTES de tentar converter para dict.
        print(f"DEBUG NEO4J CRUD: result.single() retornou: {record}") # Log 3
        if record and not record.data(): # Checa se o record está vazio
             print(f"DEBUG NEO4J CRUD: Record retornado mas está vazio (record.data() é {record.data()}). Keys: {list(record.keys())}")
             return None # Se o record estiver vazio, _node_to_dict pode falhar ou retornar None
        return _node_to_dict(record) if record else None
    except Exception as e_single:
        print(f"DEBUG NEO4J CRUD: Erro dentro de result.single() ou _node_to_dict: {e_single}")
        # traceback.print_exc() # Descomente para ver o traceback completo desta exceção interna
        raise # Re-levanta para a transação principal tratar

def inserir_filme(session: Session, filme_data: Dict[str, Any]) -> Dict[str, Any]:
    if "titulo_id" not in filme_data or not filme_data["titulo_id"]: 
        raise DataValidationError("'titulo_id' é obrigatório para inserir filme no Neo4j.")
    
    filme_id_str = str(filme_data["titulo_id"])

    # Prepara os parâmetros para o novo filme, usando _id como a propriedade principal no Neo4j
    params_para_criar = filme_data.copy()
    params_para_criar["_id"] = str(params_para_criar.pop("titulo_id"))
    #params_para_criar.pop("titulo_id", None) # Remove o original para evitar redundância se _id é o primário

    # Converte tipos para garantir consistência no Neo4j
    for key in ["ano_lancamento", "numero_votos", "duracao"]:
        if key in params_para_criar and params_para_criar[key] is not None: 
            try:
                params_para_criar[key] = int(params_para_criar[key])
            except (ValueError, TypeError):
                raise DataValidationError(f"Valor inválido para '{key}': '{params_para_criar[key]}'. Esperado um número inteiro.")
    if "nota" in params_para_criar and params_para_criar["nota"] is not None: 
        try:
            params_para_criar["nota"] = float(params_para_criar['nota'])
        except (ValueError, TypeError):
            raise DataValidationError(f"Valor inválido para 'nota': '{params_para_criar['nota']}'. Esperado um número.")
    if "generos" in params_para_criar: # Garante que é uma lista de strings
        if params_para_criar["generos"] is None:
            params_para_criar["generos"] = []
        elif not isinstance(params_para_criar["generos"], list):
            params_para_criar["generos"] = [str(g) for g in str(params_para_criar["generos"]).split(',') if g.strip()]
        else: # Já é lista, garante que os itens sejam strings
            params_para_criar["generos"] = [str(g).strip() for g in params_para_criar["generos"] if str(g).strip()]
    
    # Função de transação para verificar e criar
    def _check_and_create_film_tx(tx, id_val: str, params_val: Dict[str, Any]):
        # 1. Verifica se o filme já existe
        query_check = "MATCH (f:Filme {_id: $id}) RETURN f"
        result_check = tx.run(query_check, {"id": id_val})
        existing_film_record = result_check.single() # Pega no máximo um resultado

        if existing_film_record:
            # Se encontrou, o filme já existe!
            print(f"NEO4J CRUD DEBUG (Inserir Filme): Filme com _id '{id_val}' já existe. Levantando ItemAlreadyExistsError.") # DEBUG
            raise ItemAlreadyExistsError(f"Filme com _id '{id_val}' já existe no Neo4j.")
        
        # 2. Se não existe, cria o novo filme
        print(f"NEO4J CRUD DEBUG (Inserir Filme): Filme com _id '{id_val}' não existe. Criando novo filme...") # DEBUG
        
        # A query de criação não precisa mais de ON MATCH.
        # O timestamp_criacao é adicionado pelo Cypher.
        query_create = """
        CREATE (f:Filme)
        SET f = $params_create_val, f.timestamp_criacao = timestamp()
        RETURN f
        """
        # Os parâmetros para SET f = $params_create_val já contêm _id e todas as outras props.
        result_create = tx.run(query_create, {"params_create_val": params_val})
        created_node_record = result_create.single()
        
        if not created_node_record or not created_node_record.get("f"):
            # Isso não deveria acontecer se o CREATE foi bem-sucedido
            raise DatabaseInteractionError(f"Falha ao retornar o nó após CREATE do filme Neo4j com _id '{id_val}'.")
        
        # _node_to_dict espera um Record e sabe extrair o nó 'f' dele
        return _node_to_dict(created_node_record) 

    try:
        # session.execute_write é o método mais recente e preferido para transações de escrita
        filme_criado_props = session.execute_write(
            _check_and_create_film_tx, # A função de transação
            filme_id_str,              # Primeiro argumento para _check_and_create_film_tx (após tx)
            params_para_criar          # Segundo argumento para _check_and_create_film_tx
        )
        
        # Se _check_and_create_film_tx retornou None (vindo de _node_to_dict), é um erro.
        if not filme_criado_props: 
            # Isso pode acontecer se _node_to_dict falhar por algum motivo após uma criação bem-sucedida.
            raise DatabaseInteractionError(f"Falha inesperada ao processar o filme '{filme_id_str}' após a criação no Neo4j (filme_criado_props é None).")
        
        print(f"NEO4J CRUD INFO (Inserir Filme): Filme '{filme_id_str}' inserido com sucesso.") # DEBUG
        return filme_criado_props
        
    except ItemAlreadyExistsError: # Se _check_and_create_film_tx levantou
        raise # Re-levanta para o serviço/API tratar
    except DataValidationError: # Se a preparação dos parâmetros falhou
        raise
    except Exception as e: 
        # Captura outras exceções do driver Neo4j ou DatabaseInteractionError da sub-função
        print(f"NEO4J CRUD ERROR (Inserir Filme): Exceção ao inserir filme '{filme_id_str}': {e}") # DEBUG
        # import traceback # Para debug mais profundo
        # traceback.print_exc()
        raise DatabaseInteractionError(f"Erro ao inserir filme Neo4j _id '{filme_id_str}': {str(e)}")


def inserir_ator(session: Session, ator_data: Dict[str, Any]) -> Dict[str, Any]:
    if "ator_id" not in ator_data: raise DataValidationError("'ator_id' obrigatório.")
    params = ator_data.copy()
    params["_id"] = str(params["ator_id"])
    params.pop("ator_id", None)
    if "ano_nascimento" in params and params["ano_nascimento"] is not None:
        params["ano_nascimento"] = int(params["ano_nascimento"])
    
    query = """
    MERGE (a:Ator {_id: $_id})
    ON CREATE SET a = $params, a.timestamp_criacao = timestamp()
    ON MATCH SET a += $params_on_match, a.timestamp_atualizacao = timestamp()
    RETURN a
    """
    params_on_match = {k: v for k, v in params.items() if k != "_id"}
    try:
        ator_node_props = session.execute_write(
            _execute_write_query_single_return,
            query,
            {"_id": params["_id"], "params": params, "params_on_match": params_on_match}
        )
        if not ator_node_props:
            raise DatabaseInteractionError(f"Falha ao inserir/atualizar ator Neo4j com _id '{params['_id']}'.")
        return ator_node_props
    except Exception as e:
        if "already exists" in str(e).lower():
            raise ItemAlreadyExistsError(f"Ator com _id '{params['_id']}' já existe (Neo4j).") from e
        raise DatabaseInteractionError(f"Erro ao inserir/atualizar ator Neo4j _id '{params.get('_id')}': {e}")


def inserir_elenco(session: Session, elenco_data: Dict[str, Any]) -> Dict[str, Any]:
    if not all(k in elenco_data for k in ["ator_id", "titulo_id"]): # nome_personagem é opcional aqui
        raise DataValidationError("ator_id e titulo_id são obrigatórios para elenco.")

    # Usaremos _id do ator e _id do filme (que são ator_id e titulo_id)
    params = {
        "ator_id_param": str(elenco_data["ator_id"]),
        "filme_id_param": str(elenco_data["titulo_id"]),
        "props_relacao": {"nome_personagem": elenco_data.get("nome_personagem", "Desconhecido")}
    }
    query = """
    MATCH (a:Ator {_id: $ator_id_param})
    MATCH (f:Filme {_id: $filme_id_param})
    MERGE (a)-[r:ACTED_IN]->(f)
    SET r = $props_relacao // Isso substitui todas as propriedades da relação
    RETURN {ator_id: a._id, filme_id: f._id, nome_personagem: r.nome_personagem} as relacao
    """
    try:
        rel_info = session.execute_write(_execute_write_query_single_return, query, params)
        if not rel_info:
            raise DatabaseInteractionError("Falha ao criar/atualizar relação de elenco no Neo4j.")
        return rel_info
    except Exception as e: # Pode haver erro se o ator ou filme não existir, mas MATCH não levanta erro, apenas não encontra.
                      # A query atual não verifica se MATCH encontrou, o que pode ser um problema.
                      # Uma melhoria seria verificar se 'a' e 'f' foram encontrados antes do MERGE.
        raise DatabaseInteractionError(f"Erro ao inserir elenco Neo4j: {e}")


# --- CONSULTAS ---
def buscar_filme_por_id(session: Session, id_filme: str) -> Optional[Dict[str, Any]]:
    query = "MATCH (f:Filme {_id: $id_filme_param}) RETURN f"
    params = {"id_filme_param": str(id_filme)} # Garante que é string
    try:
        filme_node = session.execute_read(_execute_write_query_single_return, query, params)
        if not filme_node:
            raise ItemNotFoundError(f"Filme com _id '{id_filme}' não encontrado no Neo4j.")
        return filme_node
    except ItemNotFoundError: raise
    except Exception as e:
        raise DatabaseInteractionError(f"Erro ao buscar filme por _id '{id_filme}' no Neo4j: {e}")

def buscar_filmes_avancado(
    session: Session, # Recebe a sessão Neo4j
    titulo: Optional[str] = None,
    tipo: Optional[str] = None,
    ano_min: Optional[int] = None,
    generos: Optional[List[str]] = None, # Espera uma lista de strings
    nota_min: Optional[float] = None,
    duracao_min: Optional[int] = None,
    ordenar_por: str = "nota",
    ordem: int = -1,
    limite: Optional[int] = 10000, # Default do limite
    ano_corte_futuro: int = 2025 # Vem do query_service
) -> List[Dict[str, Any]]:
    
    print(f"--- NEO4J CRUD: Iniciando buscar_filmes_avancado ---") # DEBUG
    print(f"Filtros recebidos: Título='{titulo}', Tipo='{tipo}', AnoMin='{ano_min}', Gêneros='{generos}', NotaMin='{nota_min}', DuraçãoMin='{duracao_min}'") # DEBUG
    print(f"Ordenação: Por='{ordenar_por}', Ordem='{ordem}'. Limite: {limite}. Ano Corte Futuro: {ano_corte_futuro}") # DEBUG

    conditions = []
    # Usamos o 'limite' da assinatura da função. Se for None, o Cypher lida com isso (sem LIMIT) ou podemos definir um default alto.
    # Para consistência, se limite for None, não adicionaremos LIMIT à query, ou usaremos um default bem alto.
    # O parâmetro 'limite_param' no Cypher espera um valor.
    params_cypher: Dict[str, Any] = {
        "limite_param": limite if limite is not None and limite > 0 else 10000, # Default alto se limite for None ou inválido
        "ano_corte_futuro_param": ano_corte_futuro
    }

    if titulo:
        conditions.append("toLower(f.titulo) CONTAINS toLower($titulo_param)")
        params_cypher["titulo_param"] = titulo
    if tipo:
        conditions.append("f.tipo = $tipo_param")
        params_cypher["tipo_param"] = tipo
    
    if generos: # generos é Optional[List[str]] e já deve vir como lista limpa do query_service
        # Se a lista de generos não estiver vazia após a limpeza no query_service
        if any(g.strip() for g in generos): # Checa se tem algum genero não vazio na lista
             conditions.append("ALL(g IN $generos_param WHERE g IN f.generos)")
             params_cypher["generos_param"] = [g.strip() for g in generos if g.strip()] # Garante limpeza aqui tbm
        # else: Não adiciona a condição de genero se a lista estiver vazia ou só com strings vazias

    # Lógica para filtros de performance (nota, duracao) e regra do ano_corte_futuro
    # (Esta lógica é a mesma que você já tem e parece correta)
    perf_filters_cypher_conditions = [] # Renomeado para evitar conflito com a lista 'conditions' principal
    if nota_min is not None:
        perf_filters_cypher_conditions.append("f.nota >= $nota_min_param")
        params_cypher["nota_min_param"] = float(nota_min)
    
    # Aplicar duracao_min apenas se o tipo não for "jogo" (ou se o tipo não for fornecido)
    if duracao_min is not None:
        if not tipo or tipo.lower() != "jogo":
            perf_filters_cypher_conditions.append("f.duracao >= $duracao_min_param")
            params_cypher["duracao_min_param"] = int(duracao_min)
        else:
            print(f"NEO4J CRUD DEBUG: Filtro de duracao_min não aplicado pois tipo é '{tipo}'.")
    
    if perf_filters_cypher_conditions: # Somente adiciona a lógica OR complexa se houver filtros de performance
        cond_normais_com_perf = f"""
        (
            (f.ano_lancamento < $ano_corte_futuro_param OR f.ano_lancamento IS NULL) OR
            (f.ano_lancamento >= $ano_corte_futuro_param AND (f.nota > 0 OR f.numero_votos > 0)) 
        ) AND ({' AND '.join(perf_filters_cypher_conditions)})
        """
        cond_futuros_sem_aval = """
        (
            f.ano_lancamento >= $ano_corte_futuro_param AND
            (f.nota IS NULL OR f.nota = 0) AND
            (f.numero_votos IS NULL OR f.numero_votos = 0)
        )
        """
        conditions.append(f"( ({cond_normais_com_perf}) OR ({cond_futuros_sem_aval}) )")

    if ano_min is not None:
        conditions.append("f.ano_lancamento >= $ano_min_param")
        params_cypher["ano_min_param"] = int(ano_min)

    # Montagem da Query Cypher
    query_cypher_str = "MATCH (f:Filme) "
    if conditions:
        query_cypher_str += "WHERE " + " AND ".join(conditions) + " "
    
    # --- A MÁGICA DO 'WITH f' PARA GARANTIR UNICIDADE ANTES DO RETURN ---
    query_cypher_str += "WITH f " 
    query_cypher_str += "RETURN f " # O DISTINCT aqui é redundante se WITH f já garante, mas inofensivo.
                                  # Pode ser só "RETURN f"
    
    # Adicionar Ordenação e Limite
    valid_sort_fields = ["titulo", "ano_lancamento", "nota", "numero_votos", "duracao", "_id"]
    if ordenar_por not in valid_sort_fields: 
        print(f"NEO4J CRUD WARN: Campo de ordenação '{ordenar_por}' inválido para Filme. Usando 'nota' como padrão.")
        ordenar_por = "nota" 
    
    order_direction_str = "DESC" if ordem == -1 else "ASC"
    query_cypher_str += f"ORDER BY f.{ordenar_por} {order_direction_str} "
    query_cypher_str += "LIMIT $limite_param"

    #print("NEO4J CRUD DEBUG: Query FINAL MONTADA para busca avançada (com WITH f):\n", query_cypher_str)
    print("NEO4J CRUD DEBUG: Parâmetros FINAIS para busca avançada:\n", params_cypher)
    
    try:
        # _execute_read_query já itera sobre os resultados e chama _node_to_dict
        resultados = session.execute_read(_execute_read_query, query_cypher_str, params_cypher)
        print(f"NEO4J CRUD DEBUG: buscar_filmes_avancado (com WITH f) retornou {len(resultados)} filmes.")
        return resultados
    except Exception as e:
        print(f"NEO4J CRUD ERROR: Exceção em buscar_filmes_avancado (com WITH f): {e}")
        # import traceback # Para debug mais profundo se necessário
        # traceback.print_exc()
        raise DatabaseInteractionError(f"Erro ao executar busca avançada de filmes no Neo4j: {e}")

def buscar_filmes_por_ator(session: Session, id_ator: str, ordenar_por: str = 'ano_lancamento', ordem: int = -1, limite: Optional[int] = 10000) -> List[Dict[str, Any]]:
    # AGORA O PARÂMETRO É id_ator E ESPERA O "nm..."
    print(f"--- NEO4J CRUD: Iniciando buscar_filmes_por_ator para ID_ATOR: '{id_ator}' ---")
    
    order_direction = "DESC" if ordem == -1 else "ASC"
    valid_sort_fields_filme = ["titulo", "ano_lancamento", "nota", "numero_votos", "duracao", "_id"]
    if ordenar_por not in valid_sort_fields_filme: 
        print(f"NEO4J CRUD WARN: Campo de ordenação '{ordenar_por}' inválido. Usando 'ano_lancamento'.")
        ordenar_por = "ano_lancamento"

    # Etapa 1: Verificar se o ator realmente existe no banco PELO _ID
    def check_ator_exists_tx(tx: Transaction, ator_id_check: str) -> bool:
        # AGORA BUSCA PELA PROPRIEDADE _id
        query_check = "MATCH (a:Ator {_id: $id_param}) RETURN count(a) > 0 AS ator_existe"
        result = tx.run(query_check, {"id_param": ator_id_check}) # Usa id_param
        record = result.single()
        return record["ator_existe"] if record else False

    ator_realmente_existe = session.execute_read(check_ator_exists_tx, str(id_ator)) # Passa o ID
    print(f"NEO4J CRUD DEBUG: Ator com _id '{str(id_ator)}' EXISTE no DB? {ator_realmente_existe}")

    if not ator_realmente_existe:
        print(f"NEO4J CRUD INFO: Ator com _id '{str(id_ator)}' não encontrado no banco. Retornando lista vazia.")
        return []

    # Etapa 2: Montar e executar a query principal BUSCANDO POR _id
    query_cypher = f"""
    MATCH (ator:Ator {{_id: $id_ator_param}})-[r:ACTED_IN]->(filme:Filme) 
    RETURN filme, r.nome_personagem AS nome_personagem_rel
    ORDER BY filme.{ordenar_por} {order_direction}
    LIMIT $limite_param
    """
    params_cypher = {"id_ator_param": str(id_ator), "limite_param": limite} # Passa o ID como parâmetro
    
    #print(f"NEO4J CRUD DEBUG: Query Cypher (buscando por _id):\n{query_cypher}")
    print(f"NEO4J CRUD DEBUG: Parâmetros: {params_cypher}")

    # Etapa 3: Processar os resultados
    # A sua função process_results_tx interna pode continuar a mesma,
    # pois ela já espera o nó 'filme' e 'nome_personagem_rel' do resultado da query.
    def process_results_tx(tx: Transaction, q: str, p: Dict[str, Any]) -> List[Dict[str, Any]]:
        filmes_encontrados = []
        result_cursor = tx.run(q, p)
        lista_de_records = list(result_cursor)
        print(f"NEO4J CRUD DEBUG (process_results_tx): Número de records brutos: {len(lista_de_records)}")
        if lista_de_records:
            print(f"NEO4J CRUD DEBUG (process_results_tx): Exemplo do primeiro record.data(): {lista_de_records[0].data()}")
        for record_item in lista_de_records:
            filme_node_obj = record_item.get("filme")
            nome_personagem = record_item.get("nome_personagem_rel")
            if filme_node_obj:
                try:
                    filme_dict = dict(filme_node_obj)
                    # Aplicar conversões de tipo e _id (mantenha sua lógica aqui)
                    if "_id" not in filme_dict and "titulo_id" in filme_dict:
                         filme_dict["_id"] = str(filme_dict["titulo_id"])
                    elif "_id" in filme_dict:
                         filme_dict["_id"] = str(filme_dict["_id"])
                    for field_key in ["ano_lancamento", "numero_votos", "duracao"]:
                        if field_key in filme_dict and filme_dict[field_key] is not None:
                            try: filme_dict[field_key] = int(filme_dict[field_key])
                            except: pass # Seria bom logar essa falha de conversão
                    if "nota" in filme_dict and filme_dict["nota"] is not None:
                        try: filme_dict["nota"] = float(filme_dict["nota"])
                        except: pass # Seria bom logar essa falha de conversão
                    if nome_personagem is not None:
                        filme_dict["nome_personagem"] = nome_personagem
                    filmes_encontrados.append(filme_dict)
                except Exception as e_conv:
                    print(f"NEO4J CRUD WARN (process_results_tx): Falha ao converter Node: {e_conv}")
            else:
                print(f"NEO4J CRUD WARN (process_results_tx): Record sem 'filme': {record_item.data()}")
        return filmes_encontrados
    # --- Fim da definição de process_results_tx ---

    try:
        resultados_finais = session.execute_read(process_results_tx, query_cypher, params_cypher)
        print(f"--- NEO4J CRUD: buscar_filmes_por_ator (por _id) para '{id_ator}' retornou {len(resultados_finais)} filmes ---")
        return resultados_finais
    except Exception as e_main:
        print(f"NEO4J CRUD ERROR: Exceção em buscar_filmes_por_ator (por _id): {e_main}")
        # import traceback # Descomente para ver o traceback completo
        # traceback.print_exc()
        raise DatabaseInteractionError(f"Erro ao buscar filmes do ator '{id_ator}' no Neo4j: {e_main}")

def buscar_atores_por_filmes(session: Session, id_filme: str, limite: Optional[int] = 10000) -> List[Dict[str, Any]]:
    query = """
    MATCH (filme:Filme {_id: $id_filme_param})<-[r_atuou:ACTED_IN]-(ator:Ator)
    OPTIONAL MATCH (ator)-[:ACTED_IN]->(outro_filme:Filme)
    WHERE outro_filme <> filme
    WITH ator, r_atuou, COLLECT(DISTINCT outro_filme{_id: outro_filme._id, titulo: outro_filme.titulo, ano_lancamento: outro_filme.ano_lancamento}) AS outros_filmes_lista
    RETURN ator {
        .*, 
        _id: ator._id, 
        nome_personagem: r_atuou.nome_personagem, 
        outros_filmes: outros_filmes_lista
    } AS ator_detalhado
    LIMIT $limite_param
    """
    params = {"id_filme_param": str(id_filme), "limite_param": limite}
    try:
        # _execute_read_query espera que cada record seja um nó/dict principal.
        # Aqui, retornamos 'ator_detalhado'.
        result = session.execute_read(lambda tx, q, p: [r["ator_detalhado"] for r in tx.run(q, p)], query, params)
        return result
    except Exception as e:
        raise DatabaseInteractionError(f"Erro ao buscar atores do filme '{id_filme}' no Neo4j: {e}")

# --- ATUALIZAÇÃO ---
def atualizar_campo_filme(session: Session, id_filme: str, campo_para_atualizar: str, novo_valor: Any) -> Optional[Dict[str, Any]]:
    # Primeiro, verifica se o filme existe para levantar ItemNotFoundError se não
    filme_existente_check = buscar_filme_por_id(session, id_filme) # Reusa a função de busca
    # A linha acima já levanta ItemNotFoundError se não achar.

    valor_final = novo_valor
    if campo_para_atualizar in ["ano_lancamento", "numero_votos", "duracao"] and novo_valor is not None:
        try: valor_final = int(novo_valor)
        except (ValueError, TypeError): raise DataValidationError(f"Valor '{novo_valor}' inválido para campo numérico '{campo_para_atualizar}'.")
    elif campo_para_atualizar == "nota" and novo_valor is not None:
        try: valor_final = float(novo_valor)
        except (ValueError, TypeError): raise DataValidationError(f"Valor '{novo_valor}' inválido para nota.")
    elif campo_para_atualizar == "generos" and not isinstance(novo_valor, list):
        raise DataValidationError("Gêneros devem ser uma lista.")

    # Construção dinâmica da cláusula SET (CUIDADO COM INJEÇÃO SE campo_para_atualizar VEM DE INPUT DIRETO)
    # Como vem de uma lista controlada (CAMPOS_ATUALIZAVEIS_FILME), é mais seguro.
    query_update = f"""
    MATCH (f:Filme {{_id: $id_filme_param}})
    SET f.{campo_para_atualizar} = $novo_valor_param
    RETURN f
    """
    params = {"id_filme_param": str(id_filme), "novo_valor_param": valor_final}
    try:
        filme_atualizado = session.execute_write(_execute_write_query_single_return, query_update, params)
        if not filme_atualizado:
            raise DatabaseInteractionError(f"Filme '{id_filme}' foi encontrado mas falhou ao atualizar o campo '{campo_para_atualizar}'.")
        return filme_atualizado
    except Exception as e:
        raise DatabaseInteractionError(f"Erro ao atualizar campo '{campo_para_atualizar}' do filme '{id_filme}' no Neo4j: {e}")

# --- REMOÇÃO ---
def remover_filme(session: Session, id_filme: str) -> bool:
    # Verificar existência primeiro
    filme_existente_check = buscar_filme_por_id(session, id_filme) # Levanta ItemNotFoundError se não existe

    query_delete = "MATCH (f:Filme {_id: $id_filme_param}) DETACH DELETE f"
    params = {"id_filme_param": str(id_filme)}
    try:
        def delete_transaction(tx, q, p):
            result = tx.run(q, p)
            summary = result.consume()
            return summary.counters.nodes_deleted > 0 # Retorna True se nós foram deletados

        foi_deletado = session.execute_write(delete_transaction, query_delete, params)
        if not foi_deletado: # Pode acontecer se o filme foi deletado entre o check e o delete (race condition)
                         # ou se DETACH DELETE não afetou nós por algum motivo (ex: filme não encontrado no MATCH do delete)
            # Re-verificar se ainda existe para ter certeza
            if buscar_filme_por_id(session, id_filme) is not None:
                 raise DatabaseInteractionError(f"Filme '{id_filme}' não foi efetivamente removido após a operação de delete no Neo4j.")
            # Se não existe mais, a deleção pode ter ocorrido ou o check inicial falhou em reter o estado.
            # Consideramos sucesso se não existe mais. Ou, se o delete_transaction retornou True.
            # A lógica do delete_transaction já retorna True se nodes_deleted > 0.
        return True # Se chegou aqui sem exceção do delete_transaction ou do check, e foi_deletado é True
    except ItemNotFoundError: # Se o check inicial já falhou
        raise
    except Exception as e:
        raise DatabaseInteractionError(f"Erro ao remover filme '{id_filme}' no Neo4j: {e}")

# --- AGREGAÇÕES ---
def contagem_por_ano(session: Session) -> List[Dict[str, Any]]:
    query = """
    MATCH (f:Filme)
    WHERE f.ano_lancamento IS NOT NULL
    RETURN f.ano_lancamento AS ano, count(f) AS quantidade
    ORDER BY ano ASC
    """
    try:
        # _execute_read_query pode precisar de ajuste se o retorno não for um nó principal
        # Aqui, o retorno é {ano: ..., quantidade: ...}
        def _execute_aggregation_query(tx: Transaction, q: str, p: Dict = None) -> List[Dict[str, Any]]:
            result = tx.run(q, p if p else {})
            return [record.data() for record in result] # .data() converte o Record para dict

        return session.execute_read(_execute_aggregation_query, query)
    except Exception as e:
        raise DatabaseInteractionError(f"Erro ao contar filmes por ano no Neo4j: {e}")

def media_notas_por_genero(session: Session) -> List[Dict[str, Any]]:
    query = """
    MATCH (f:Filme)
    WHERE f.nota IS NOT NULL AND f.generos IS NOT NULL AND size(f.generos) > 0
    UNWIND f.generos AS genero_individual
    WITH genero_individual, avg(f.nota) AS media_da_nota
    RETURN genero_individual AS genero, round(media_da_nota * 10) / 10.0 AS media_nota // Arredonda para 1 casa decimal
    ORDER BY media_nota DESC, genero ASC
    """
    try:
        def _execute_aggregation_query(tx: Transaction, q: str, p: Dict = None) -> List[Dict[str, Any]]:
            result = tx.run(q, p if p else {})
            return [record.data() for record in result]

        return session.execute_read(_execute_aggregation_query, query)
    except Exception as e:
        raise DatabaseInteractionError(f"Erro ao calcular média de notas por gênero no Neo4j: {e}")

# --- CARGA DE DADOS ---
def carregar_dados_neo4j(
    driver: Driver, # Recebe o driver para gerenciar sessões e transações em lote
    filmes_path: str,
    atores_path: str,
    elenco_path: str
) -> Dict[str, Any]:
    
    erros_carga = []
    counts = {"filmes": 0, "atores": 0, "elenco": 0}

    # Constraints (executar uma vez, idealmente no setup da aplicação)
    with driver.session(database="neo4j") as session: # Especificar database se não for o default
        try:
            session.run("CREATE CONSTRAINT cons_filme_id IF NOT EXISTS FOR (f:Filme) REQUIRE f._id IS UNIQUE")
            session.run("CREATE CONSTRAINT cons_ator_id IF NOT EXISTS FOR (a:Ator) REQUIRE a._id IS UNIQUE")
            # Índices para performance de MATCH
            session.run("CREATE INDEX idx_filme_titulo IF NOT EXISTS FOR (f:Filme) ON (f.titulo)")
            session.run("CREATE INDEX idx_filme_ano IF NOT EXISTS FOR (f:Filme) ON (f.ano_lancamento)")
            session.run("CREATE INDEX idx_ator_nome IF NOT EXISTS FOR (a:Ator) ON (a.nome_ator)")
        except Exception as e:
            print(f"AVISO NEO4J: Problema ao criar constraints/índices (pode ser normal se já existem): {e}")
    
    # Limpeza opcional
    with driver.session(database="neo4j") as session:
        session.run("MATCH (n) DETACH DELETE n")
        print("Neo4j limpo.")

    # Carregar Filmes em Lotes
    try:
        filmes_df = pd.read_csv(filmes_path, sep="\t", keep_default_na=False, na_values=['\\N', ''])
        filmes_list_of_dicts = []
        for idx, row in filmes_df.iterrows():
            try:
                filme_pydantic_data = { # Para validação Pydantic, que espera titulo_id
                    'titulo_id': str(row['titulo_id']),
                    'titulo': str(row['titulo']) if pd.notna(row['titulo']) else None,
                    'tipo': str(row['tipo']) if pd.notna(row['tipo']) else None,
                    'ano_lancamento': int(row['ano_lancamento']) if pd.notna(row['ano_lancamento']) and row['ano_lancamento'] != '' else None,
                    'generos': _limpar_generos_cypher(row.get('generos')),
                    'nota': float(row['nota']) if pd.notna(row['nota']) and row['nota'] != '' else None,
                    'numero_votos': int(row['numero_votos']) if pd.notna(row['numero_votos']) and row['numero_votos'] != '' else None,
                    'duracao': int(row['duracao']) if pd.notna(row['duracao']) and row['duracao'] != '' else None,
                    'sinopse': str(row['sinopse']) if pd.notna(row['sinopse']) else None
                }
                _ = Filme(**filme_pydantic_data) # Valida
                
                # Para o Neo4j, vamos usar _id
                filme_neo4j_props = filme_pydantic_data.copy()
                filme_neo4j_props["_id"] = filme_neo4j_props.pop("titulo_id")
                filmes_list_of_dicts.append(filme_neo4j_props)
            except Exception as e_val:
                erros_carga.append({"arq":"filmes_neo", "ln":idx+2, "id":row.get('titulo_id'), "err":"Val/Prep", "det":str(e_val)})

        if filmes_list_of_dicts:
            query_filme_unwind = """
            UNWIND $batch as props
            MERGE (f:Filme {_id: props._id})
            SET f.titulo = props.titulo, 
                f.tipo = props.tipo, 
                f.ano_lancamento = props.ano_lancamento,
                f.generos = props.generos,
                f.nota = props.nota,
                f.numero_votos = props.numero_votos,
                f.duracao = props.duracao,
                f.sinopse = props.sinopse,
                f.titulo_id = props._id // Mantém titulo_id como propriedade também, se útil
            RETURN count(f) as count
            """
            with driver.session(database="neo4j") as session:
                batch_size = 1000
                for i in range(0, len(filmes_list_of_dicts), batch_size):
                    batch = filmes_list_of_dicts[i:i + batch_size]
                    res = session.run(query_filme_unwind, batch=batch)
                    counts["filmes"] += res.single(strict=True)['count']
    except Exception as e_read:
        erros_carga.append({"arq": "filmes_neo", "tipo": "LeituraDF/Carga", "det": str(e_read)})

    # Carregar Atores (similar)
    try:
        atores_df = pd.read_csv(atores_path, sep="\t", keep_default_na=False, na_values=['\\N', ''])
        atores_list_of_dicts = []
        # ... (lógica de loop, validação Pydantic, preparação do dict com _id = ator_id)
        for idx, row in atores_df.iterrows():
            try:
                ator_pydantic_data = {
                    'ator_id': str(row['ator_id']),
                    'nome_ator': str(row['nome_ator']) if pd.notna(row['nome_ator']) else None,
                    'ano_nascimento': int(row['ano_nascimento']) if pd.notna(row['ano_nascimento']) and row['ano_nascimento'] != '' else None
                }
                _ = Ator(**ator_pydantic_data)
                ator_neo4j_props = ator_pydantic_data.copy()
                ator_neo4j_props["_id"] = ator_neo4j_props.pop("ator_id")
                atores_list_of_dicts.append(ator_neo4j_props)
            except Exception as e_val:
                erros_carga.append({"arq":"atores_neo", "ln":idx+2, "id":row.get('ator_id'), "err":"Val/Prep", "det":str(e_val)})
        
        if atores_list_of_dicts:
            query_ator_unwind = """
            UNWIND $batch as props
            MERGE (a:Ator {_id: props._id})
            SET a.nome_ator = props.nome_ator,
                a.ano_nascimento = props.ano_nascimento,
                a.ator_id = props._id
            RETURN count(a) as count
            """
            with driver.session(database="neo4j") as session:
                batch_size = 1000
                for i in range(0, len(atores_list_of_dicts), batch_size):
                    batch = atores_list_of_dicts[i:i + batch_size]
                    res = session.run(query_ator_unwind, batch=batch)
                    counts["atores"] += res.single(strict=True)['count']
    except Exception as e_read:
        erros_carga.append({"arq": "atores_neo", "tipo": "LeituraDF/Carga", "det": str(e_read)})

    # Carregar Elenco (Relações)
    try:
        elenco_df = pd.read_csv(elenco_path, sep="\t", keep_default_na=False, na_values=['\\N', ''])
        elenco_list_of_relations = []
        # ... (lógica de loop, validação Pydantic)
        for idx, row in elenco_df.iterrows():
            try:
                elenco_data = {
                    'ator_id_param': str(row['ator_id']), # _id do Ator
                    'filme_id_param': str(row['titulo_id']), # _id do Filme
                    'props_rel': {'nome_personagem': str(row['nome_personagem']) if pd.notna(row['nome_personagem']) else "Desconhecido"}
                }
                _ = Elenco(ator_id=elenco_data['ator_id_param'], titulo_id=elenco_data['filme_id_param'], nome_personagem=elenco_data['props_rel']['nome_personagem'])
                elenco_list_of_relations.append(elenco_data)
            except Exception as e_val:
                erros_carga.append({"arq":"elenco_neo", "ln":idx+2, "ids":f"A:{row.get('ator_id')},F:{row.get('titulo_id')}", "err":"Val/Prep", "det":str(e_val)})
        
        if elenco_list_of_relations:
            query_elenco_unwind = """
            UNWIND $batch as rel
            MATCH (a:Ator {_id: rel.ator_id_param})
            MATCH (f:Filme {_id: rel.filme_id_param})
            MERGE (a)-[r:ACTED_IN]->(f)
            SET r = rel.props_rel // Sobrescreve/define propriedades da relação
            RETURN count(r) as count
            """
            with driver.session(database="neo4j") as session:
                batch_size = 1000
                for i in range(0, len(elenco_list_of_relations), batch_size):
                    batch = elenco_list_of_relations[i:i + batch_size]
                    res = session.run(query_elenco_unwind, batch=batch)
                    counts["elenco"] += res.single(strict=True)['count']
    except Exception as e_read:
        erros_carga.append({"arq": "elenco_neo", "tipo": "LeituraDF/Carga", "det": str(e_read)})
    
    msg = f"Carga Neo4j: {counts['filmes']} filmes, {counts['atores']} atores, {counts['elenco']} relações."
    status_op = "sucesso" if not erros_carga else "concluído_com_erros"
    if erros_carga: print(f"LOG DE CARGA NEO4J - Erros: {erros_carga}")
    return {
        "status": status_op, "message": msg,
        "detalhes_carga": counts,
        "erros_execucao": erros_carga
    }