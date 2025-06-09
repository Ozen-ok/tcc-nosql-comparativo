# src/databases/cassandra/crud.py
from cassandra.cluster import Session
from cassandra.query import SimpleStatement
from cassandra.encoder import Encoder # Para lidar com tipos complexos se necessário
import pandas as pd
from typing import List, Dict, Any, Optional
import re # Para limpar_generos_cassandra

# Importa as exceções centralizadas
from src.core.exceptions import (
    DatabaseOperationError, # Embora não estejamos usando diretamente, é bom ter se precisar no futuro
    ItemNotFoundError,
    ItemAlreadyExistsError,
    DataValidationError, # Se for usar validação Pydantic aqui dentro
    DatabaseInteractionError,
    ValidationError
)

# Importa os modelos Pydantic para usar em carregar_dados e para referência de estrutura
from src.models.filme import Filme as FilmeModelPydantic # Renomeando para evitar conflito de nome
from src.models.ator import Ator as AtorModelPydantic
from src.models.elenco import Elenco as ElencoModelPydantic

# --- FUNÇÕES AUXILIARES ---

def _ordenar_e_filtrar_resultados_cassandra(
    resultados_brutos: List[Dict[str, Any]],
    filtros_python: Dict[str, Any], # Filtros a serem aplicados em Python
    ordenar_por: Optional[str],
    ordem: int, # 1 para ASC, -1 para DESC
    limite_final: int
) -> List[Dict[str, Any]]:
    """
    Aplica filtros adicionais em Python e ordena os resultados.
    Cassandra é limitado em suas capacidades de filtragem e ordenação via CQL diretamente
    em colunas que não fazem parte da chave primária ou de índices específicos.
    """
    print(f"DEBUG CASSANDRA (_ordenar_e_filtrar): Recebido {len(resultados_brutos)} resultados brutos para filtrar.")
    print(f"DEBUG CASSANDRA (_ordenar_e_filtrar): Aplicando filtros_python: {filtros_python}")
    
    resultados_filtrados = []
    if filtros_python:
        for item_idx, item in enumerate(resultados_brutos): # Adicionado item_idx para log
            match = True
            for campo_filtro_py, valor_filtro_py in filtros_python.items():
                # Lógica de filtro (como já estava no seu arquivo)
                # Adapte esta lógica conforme os tipos de filtros que você precisa
                if campo_filtro_py == "titulo_contem" and isinstance(valor_filtro_py, str):
                    if valor_filtro_py.lower() not in item.get("titulo", "").lower():
                        match = False; break
                elif campo_filtro_py == "generos_contem_todos" and isinstance(valor_filtro_py, list) and valor_filtro_py:
                    if not set(valor_filtro_py).issubset(set(item.get("generos", []))):
                        match = False; break
                # ... (outros filtros python que você tem) ...
            
            if match:
                resultados_filtrados.append(item)
            # else: # Descomente para log detalhado de itens não passando no filtro
            #     print(f"DEBUG CASSANDRA (_ordenar_e_filtrar): Item {item_idx} (ID: {item.get('_id', 'N/A')}) REPROVADO pelos filtros Python.")
    else:
        resultados_filtrados = list(resultados_brutos) # Garante que é uma cópia se não houver filtros

    # Ordenação em Python
    if ordenar_por:
        def chave_ordem(x):
            val = x.get(ordenar_por)
            if val is None:
                # Define valor padrão para None para ordenação consistente
                if isinstance(x.get(ordenar_por), (int, float)): # Exemplo, pode precisar de mais tipos
                    return float('-inf') if ordem == 1 else float('inf')
                return "" # Para strings ou outros tipos
            # Tenta converter para float para ordenação numérica (ex: nota, ano)
            try: return float(val)
            except (ValueError, TypeError): return str(val).lower() # Fallback para string (lower para case-insensitive)
        
        try:
            resultados_filtrados.sort(key=chave_ordem, reverse=(ordem == -1))
        except TypeError as te:
            print(f"AVISO CASSANDRA: TypeError durante ordenação Python para o campo '{ordenar_por}': {te}")
    print(f"DEBUG CASSANDRA (_ordenar_e_filtrar): Retornando {len(resultados_filtrados)} resultados após filtros Python e ordenação.")
    return resultados_filtrados[:limite_final]


def _buscar_ator_id_por_identificador_cassandra(
    session: Session, 
    identificador_ator: str, 
    atores_tabela: str = "atores"
) -> Optional[str]:
    """
    Busca o ator_id no Cassandra. Tenta primeiro como PK (ator_id), 
    depois por nome_ator (pode exigir ALLOW FILTERING ou índice secundário).
    """
    print(f"DEBUG CASSANDRA: Buscando ator com identificador: '{identificador_ator}' na tabela '{atores_tabela}'")
    
    # Tentativa 1: Buscar como se fosse ator_id (PK)
    query_por_id_str = f"SELECT ator_id FROM {atores_tabela} WHERE ator_id = %s"
    try:
        statement_id = SimpleStatement(query_por_id_str)
        row_id = session.execute(statement_id, (identificador_ator,)).one()
        if row_id:
            print(f"DEBUG CASSANDRA: Ator encontrado por ator_id (PK) '{identificador_ator}'")
            return row_id.ator_id
    except Exception as e:
        print(f"DEBUG CASSANDRA: Exceção ao buscar ator por ator_id (PK) '{identificador_ator}': {e}")

    # Tentativa 2: Buscar como se fosse nome_ator
    # Esta query provavelmente precisará de "ALLOW FILTERING" ou um índice secundário em nome_ator.
    # Para o TCC, com dados pequenos, ALLOW FILTERING é aceitável, mas mencione as implicações.
    query_por_nome_str = f"SELECT ator_id FROM {atores_tabela} WHERE nome_ator = %s ALLOW FILTERING"
    try:
        statement_nome = SimpleStatement(query_por_nome_str)
        # Cassandra pode retornar múltiplos atores se o nome não for único e não houver índice único.
        # Pegamos o primeiro encontrado para simplificar, mas em um sistema real, isso precisaria de mais tratamento.
        row_nome = session.execute(statement_nome, (identificador_ator,)).one() 
        if row_nome:
            print(f"DEBUG CASSANDRA: Ator encontrado por nome_ator '{identificador_ator}', ID retornado: {row_nome.ator_id}")
            return row_nome.ator_id
    except Exception as e:
        print(f"DEBUG CASSANDRA: Exceção ao buscar ator por nome_ator '{identificador_ator}': {e}")

    print(f"DEBUG CASSANDRA: Ator com identificador '{identificador_ator}' não encontrado (nem como _id, nem como nome_ator).")
    return None


# --- INSERÇÕES ---

def inserir_filme(session: Session, filme_data: Dict[str, Any]) -> str:
    """Insere um filme. Levanta ItemAlreadyExistsError ou DatabaseInteractionError."""
    # Validação Pydantic (opcional aqui, mas bom se os dados vierem de fontes não confiáveis)
    # try:
    #     Filme(**filme_data) # Garante que os campos básicos existem, mas não que são válidos para Cassandra
    # except ValidationError as e:
    #     raise DataValidationError(message="Dados do filme inválidos.", errors=e.errors())

    titulo_id = filme_data.get("titulo_id")
    if not titulo_id:
        raise DataValidationError("titulo_id é obrigatório para inserir filme.")

    # Verificar se o filme já existe (titulo_id é a PK)
    check_query = SimpleStatement(f"SELECT titulo_id FROM filmes WHERE titulo_id = %s")
    try:
        if session.execute(check_query, (titulo_id,)).one():
            raise ItemAlreadyExistsError(f"Filme com titulo_id '{titulo_id}' já existe no Cassandra.")

        # Preparar os dados para inserção
        # Cassandra lida bem com 'None' para campos não fornecidos, se a tabela permitir.
        # Listas como 'generos' são inseridas diretamente.
        insert_query_str = """
        INSERT INTO filmes (titulo_id, titulo, tipo, ano_lancamento, generos, nota, numero_votos, duracao, sinopse)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            filme_data.get("titulo_id"),
            filme_data.get("titulo"),
            filme_data.get("tipo"),
            filme_data.get("ano_lancamento"), # Deve ser int ou None
            list(filme_data.get("generos", [])) if filme_data.get("generos") is not None else None, # Garante que é lista ou None
            float(filme_data.get("nota")) if filme_data.get("nota") is not None else None,
            int(filme_data.get("numero_votos")) if filme_data.get("numero_votos") is not None else None,
            int(filme_data.get("duracao")) if filme_data.get("duracao") is not None else None,
            filme_data.get("sinopse")
        )
        session.execute(SimpleStatement(insert_query_str), params)
        return filme_data
    except ItemAlreadyExistsError: # Re-levantar para não ser pego pelo Exception genérico abaixo
        raise
    except Exception as e:
        raise DatabaseInteractionError(f"Erro ao inserir filme '{titulo_id}' no Cassandra: {e}")


def inserir_ator(session: Session, ator_data: Dict[str, Any]) -> str:
    """Insere um ator. Levanta ItemAlreadyExistsError ou DatabaseInteractionError."""
    ator_id = ator_data.get("ator_id")
    if not ator_id:
        raise DataValidationError("ator_id é obrigatório para inserir ator.")

    check_query = SimpleStatement(f"SELECT ator_id FROM atores WHERE ator_id = %s")
    try:
        if session.execute(check_query, (ator_id,)).one():
            raise ItemAlreadyExistsError(f"Ator com ator_id '{ator_id}' já existe no Cassandra.")

        query_str = """
        INSERT INTO atores (ator_id, nome_ator, ano_nascimento)
        VALUES (%s, %s, %s)
        """
        params = (
            ator_data.get("ator_id"),
            ator_data.get("nome_ator"),
            int(ator_data.get("ano_nascimento")) if ator_data.get("ano_nascimento") is not None else None
        )
        session.execute(SimpleStatement(query_str), params)
        return ator_id
    except ItemAlreadyExistsError:
        raise
    except Exception as e:
        raise DatabaseInteractionError(f"Erro ao inserir ator '{ator_id}' no Cassandra: {e}")


def inserir_elenco(session: Session, relacao_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Insere uma relação de elenco. Retorna os dados da relação em caso de sucesso.
    Levanta DatabaseInteractionError ou ItemAlreadyExistsError (se PK composta for violada).
    A tabela elenco tem PK ((ator_id), titulo_id, nome_personagem)
    """
    ator_id = relacao_data.get("ator_id")
    titulo_id = relacao_data.get("titulo_id")
    nome_personagem = relacao_data.get("nome_personagem")

    if not all([ator_id, titulo_id, nome_personagem]): # nome_personagem pode ser string vazia, mas não None
        raise DataValidationError("ator_id, titulo_id e nome_personagem são obrigatórios para elenco.")

    # Opcional: Verificar se a relação exata já existe (se a PK deve ser estritamente única)
    # Isso depende da modelagem e se MERGE (upsert) é aceitável ou não.
    # INSERT não fará upsert, falhará se a PK existir.
    # SELECT ator_id FROM elenco WHERE ator_id = %s AND titulo_id = %s AND nome_personagem = %s
    
    try:
        query_str = """
        INSERT INTO elenco (ator_id, titulo_id, nome_personagem) 
        VALUES (%s, %s, %s)
        """
        params = (ator_id, titulo_id, nome_personagem)
        session.execute(SimpleStatement(query_str), params)
        return relacao_data # Retorna os dados inseridos para consistência com Mongo
    except Exception as e: # Cassandra levanta erro se a PK já existir (não faz upsert com INSERT)
        # Analisar o tipo de erro para diferenciar "já existe" de outros erros de DB.
        # Por ex, "InvalidQuery: PRIMARY KEY part nome_personagem found in SET part" não é "já existe".
        # "AlreadyExists: Error creating table..." é diferente.
        # Se for um erro de violação de PK, poderia ser ItemAlreadyExistsError.
        # Por ora, um erro genérico de interação.
        raise DatabaseInteractionError(f"Erro ao inserir relação de elenco para ator '{ator_id}' e filme '{titulo_id}' no Cassandra: {e}")


# --- CONSULTAS ---

def buscar_filme_por_id(session: Session, titulo_id: str, tabela: str = "filmes") -> Optional[Dict[str, Any]]:
    """Busca um filme pelo seu titulo_id (PK). Retorna dict compatível com FilmeResponse ou None."""
    query_str = f"SELECT * FROM {tabela} WHERE titulo_id = %s" # Usando %s
    try:
        # Passando a query string e params diretamente para execute, como funcionou no teste
        result = session.execute(query_str, (titulo_id,))
        row = result.one()
        if row:
            filme_dict_raw = row._asdict()
            # Mapeamento para FilmeResponse
            return {
                "_id": filme_dict_raw.get("titulo_id"), # Para o alias 'id' no FilmeResponse
                "titulo_id": filme_dict_raw.get("titulo_id"), # Pode manter se útil
                "titulo": filme_dict_raw.get("titulo"),
                "tipo": filme_dict_raw.get("tipo"),
                "ano_lancamento": filme_dict_raw.get("ano_lancamento"),
                "generos": filme_dict_raw.get("generos", []), # Default lista vazia
                "nota": filme_dict_raw.get("nota"),
                "numero_votos": filme_dict_raw.get("numero_votos"),
                "duracao": filme_dict_raw.get("duracao"),
                "sinopse": filme_dict_raw.get("sinopse")
            }
        return None 
    except Exception as e:
        raise DatabaseInteractionError(f"Erro ao buscar filme por ID '{titulo_id}' no Cassandra: {repr(e)}")

# src/databases/cassandra/crud.py

def buscar_ator_por_id(session: Session, ator_id: str, tabela: str = "atores") -> Optional[Dict[str, Any]]:
    """Busca um ator pelo seu ator_id (PK). Retorna dict compatível com AtorResponse ou None."""
    query_str = f"SELECT * FROM {tabela} WHERE ator_id = %s" # Usando %s
    try:
        result = session.execute(query_str, (ator_id,))
        row = result.one()
        if row:
            ator_dict_raw = row._asdict()
            # Mapeamento para AtorResponse
            return {
                "_id": ator_dict_raw.get("ator_id"), # Para o alias 'id' no AtorResponse
                "ator_id": ator_dict_raw.get("ator_id"), # Pode manter
                "nome_ator": ator_dict_raw.get("nome_ator"),
                "ano_nascimento": ator_dict_raw.get("ano_nascimento")
                # Adicionar 'nome_personagem' e 'outros_filmes' se esta função fosse para esse contexto
            }
        return None
    except Exception as e:
        raise DatabaseInteractionError(f"Erro ao buscar ator por ID '{ator_id}' no Cassandra: {repr(e)}")

# src/databases/cassandra/crud.py
# ... (importações, exceções, _limpar_generos_cassandra) ...

# Função auxiliar para aplicar filtros Python com a nova regra
def _aplicar_filtros_python_cassandra_com_regra_futuro(
    filme: Dict[str, Any],
    filtros_python: Dict[str, Any],
    ano_corte_futuro: int = 2025
) -> bool:
    """Aplica filtros Python a um filme, considerando a regra para filmes futuros."""
    if not filtros_python:
        return True # Passa se não há filtros Python

    match = True
    ano_lancamento_filme = filme.get("ano_lancamento")
    nota_filme = filme.get("nota")
    votos_filme = filme.get("numero_votos") # Supondo que você tenha esse campo

    # Verifica se é um filme "futuro/sem avaliação"
    is_filme_futuro_sem_avaliacao = False
    if ano_lancamento_filme is not None and ano_lancamento_filme >= ano_corte_futuro:
        if (nota_filme is None or nota_filme == 0 or nota_filme == 0.0) and \
           (votos_filme is None or votos_filme == 0):
            is_filme_futuro_sem_avaliacao = True

    for campo_filtro, valor_filtro in filtros_python.items():
        if valor_filtro is None: continue # Ignora filtros com valor None

        # Campos que são sempre aplicados
        if campo_filtro == "titulo_contem" and isinstance(valor_filtro, str):
            if valor_filtro.lower() not in str(filme.get("titulo", "")).lower():
                match = False; break
        elif campo_filtro == "tipo" and isinstance(valor_filtro, str):
            if filme.get("tipo", "").lower() != valor_filtro.lower():
                match = False; break
        elif campo_filtro == "generos_contem_todos" and isinstance(valor_filtro, list) and valor_filtro:
            filme_generos_set = set(g.lower() for g in filme.get("generos", []))
            filtro_generos_set = set(g.lower() for g in valor_filtro)
            if not filtro_generos_set.issubset(filme_generos_set):
                match = False; break
        elif campo_filtro == "ano_lancamento_min" and isinstance(valor_filtro, int): # Filtro de ano_min sempre aplicado
            if ano_lancamento_filme is None or ano_lancamento_filme < valor_filtro:
                match = False; break
        
        # Campos de "performance" que são ignorados para filmes futuros sem avaliação
        elif campo_filtro in ["nota_min", "numero_votos_min", "duracao_min"]:
            if is_filme_futuro_sem_avaliacao:
                continue # Pula este filtro para filmes futuros sem avaliação

            # Aplica o filtro normalmente para outros filmes
            if campo_filtro == "nota_min" and isinstance(valor_filtro, (int, float)):
                if nota_filme is None or nota_filme < valor_filtro:
                    match = False; break
            # elif campo_filtro == "numero_votos_min" and isinstance(valor_filtro, int):
            #     if votos_filme is None or votos_filme < valor_filtro:
            #         match = False; break
            elif campo_filtro == "duracao_min" and isinstance(valor_filtro, int):
                tipo_filme = filme.get("tipo", "").lower()
                if tipo_filme != "jogo": # Duração não se aplica a jogos
                    if filme.get("duracao") is None or filme.get("duracao") < valor_filtro:
                        match = False; break
        else:
            # Filtro desconhecido, pode logar ou ignorar
            #print(f"AVISO CASSANDRA: Filtro Python desconhecido '{campo_filtro}'")
            pass

    return match


def _ordenar_e_filtrar_resultados_cassandra_com_regra(
    resultados_brutos: List[Dict[str, Any]],
    filtros_python: Dict[str, Any],
    ordenar_por: Optional[str],
    ordem: int,
    limite_final: int,
    ano_corte_futuro: int = 2025
) -> List[Dict[str, Any]]:
    
    resultados_filtrados_py = [
        filme for filme in resultados_brutos 
        if _aplicar_filtros_python_cassandra_com_regra_futuro(filme, filtros_python, ano_corte_futuro)
    ]
    
    if ordenar_por and resultados_filtrados_py:
        # ... (lógica de ordenação como antes) ...
        def sort_key(item):
            value = item.get(ordenar_por)
            if value is None: return float('-inf') if ordem == 1 else float('inf')
            return value
        try:
            resultados_filtrados_py.sort(key=sort_key, reverse=(ordem == -1))
        except TypeError:
            print(f"Aviso CASSANDRA: TypeError durante ordenação Python para o campo '{ordenar_por}'.")

    return resultados_filtrados_py[:limite_final]

import os,json

def buscar_filmes_avancado(
    session: Session,
    tabela: str = "filmes",
    filtros_cql: Optional[Dict[str, Any]] = None,
    filtros_python: Optional[Dict[str, Any]] = None, # Este dict terá as chaves do FiltrosBuscaAvancadaPayload
    ordenar_por: Optional[str] = "nota",
    ordem: int = -1,
    limite: int = 10000,
    limite_fetch_cassandra: int = 5000, # Aumentar para ter mais chance de pegar filmes futuros
    salvar_em_arquivo: bool = False,
    nome_arquivo_debug: str = "debug_resultados_cassandra.json",
    ano_corte_futuro_param: int = 2025 # Passado do serviço
) -> List[Dict[str, Any]]:
    # ... (lógica para construir query_base_str e executar a query CQL como antes) ...
    # ... (para pegar resultados_brutos_do_cassandra) ...
    query_base_str = f"SELECT * FROM {tabela}"
    cql_conditions = []
    cql_values = []

    if filtros_cql: # Filtros que podem ser aplicados diretamente no WHERE do Cassandra
        for campo, valor in filtros_cql.items():
            cql_conditions.append(f"{campo} = %s")
            cql_values.append(valor)
    
    if cql_conditions:
        query_base_str += " WHERE " + " AND ".join(cql_conditions)
    
    # Tentamos buscar um pouco mais para a filtragem Python, especialmente se filtros CQL são poucos
    query_base_str += f" LIMIT {limite_fetch_cassandra} ALLOW FILTERING" 
    
    resultados_brutos_do_cassandra = [] 
    try:
        statement = SimpleStatement(query_base_str, fetch_size=min(100, limite_fetch_cassandra))
        rows = session.execute(statement, tuple(cql_values))
        for row_object in rows: 
            filme_dict_convertido = row_object._asdict()
            resultados_brutos_do_cassandra.append({
                "_id": filme_dict_convertido.get("titulo_id"), 
                **filme_dict_convertido 
            })
    except Exception as e: 
        raise DatabaseInteractionError(f"Erro ao executar busca base no Cassandra: {repr(e)}") from e

    # Filtros Python agora usam a nova função com a regra
    final_results = _ordenar_e_filtrar_resultados_cassandra_com_regra(
        resultados_brutos_do_cassandra,
        filtros_python.copy() if filtros_python else {},
        ordenar_por,
        ordem,
        limite,
        ano_corte_futuro=ano_corte_futuro_param # Passa o ano de corte
    )

    # ... (lógica para salvar em arquivo, como antes) ...
    if salvar_em_arquivo and final_results:
        try:
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
            debug_output_folder = os.path.join(project_root, "debug_output_crud")
            os.makedirs(debug_output_folder, exist_ok=True)
            full_file_path = os.path.join(debug_output_folder, nome_arquivo_debug)
            with open(full_file_path, 'w', encoding='utf-8') as f:
                json.dump(final_results, f, ensure_ascii=False, indent=4)
            print(f"DEBUG CASSANDRA: Resultados da busca avançada salvos em '{full_file_path}'")
        except Exception as e:
            print(f"DEBUG CASSANDRA ERRO: Falha ao salvar resultados em arquivo: {e}")

    return final_results


def buscar_filmes_por_ator(
    session: Session, 
    identificador_ator: str, 
    atores_tabela: str = "atores", 
    elenco_tabela: str = "elenco", 
    filmes_tabela: str = "filmes", 
    ordenar_por: str = 'ano_lancamento', 
    ordem: int = -1, 
    limite: int = 10000
) -> List[Dict[str, Any]]:
    print(f"--- Iniciando buscar_filmes_por_ator (Cassandra) para identificador: {identificador_ator} ---")

    ator_id_encontrado = _buscar_ator_id_por_identificador_cassandra(session, identificador_ator, atores_tabela)

    if not ator_id_encontrado:
        return [] 

    print(f"DEBUG CASSANDRA: Buscando filmes para ator_id: {ator_id_encontrado}")

    try:
        # 1. Buscar titulo_ids da tabela elenco
        # Assumindo que ator_id é chave de partição ou tem índice em 'elenco' para esta query ser eficiente.
        # Se não, ALLOW FILTERING pode ser necessário.
        query_elenco_str = f"SELECT titulo_id FROM {elenco_tabela} WHERE ator_id = %s"
        elenco_rows = session.execute(SimpleStatement(query_elenco_str), (ator_id_encontrado,))
        titulo_ids = list(set([row.titulo_id for row in elenco_rows])) # set para remover duplicatas se houver

        if not titulo_ids:
            print(f"DEBUG CASSANDRA: Nenhum filme encontrado no elenco para o ator_id: {ator_id_encontrado}")
            return []

        if len(titulo_ids) > 100: # Limite para evitar queries IN muito grandes
            print(f"AVISO CASSANDRA: Ator {ator_id_encontrado} tem {len(titulo_ids)} filmes. Limitando busca IN a 100 títulos para performance.")
            titulo_ids = titulo_ids[:100]

        # 2. Buscar detalhes dos filmes
        placeholders = ', '.join(['%s'] * len(titulo_ids))
        query_filmes_str = f"SELECT * FROM {filmes_tabela} WHERE titulo_id IN ({placeholders})"
        
        filmes_rows = session.execute(SimpleStatement(query_filmes_str), tuple(titulo_ids))
        filmes_formatados = []
        for row in filmes_rows:
            filme_dict_raw = row._asdict()
            # Mapeamento para garantir compatibilidade com FilmeResponse
            filme_mapeado = {
                "_id": filme_dict_raw.get("titulo_id"), # <--- ADICIONA O CAMPO _id
                "titulo_id": filme_dict_raw.get("titulo_id"),
                "titulo": filme_dict_raw.get("titulo"),
                "tipo": filme_dict_raw.get("tipo"),
                "ano_lancamento": filme_dict_raw.get("ano_lancamento"),
                "generos": filme_dict_raw.get("generos", []), # Default para lista vazia se None
                "nota": filme_dict_raw.get("nota"),
                "numero_votos": filme_dict_raw.get("numero_votos"),
                "duracao": filme_dict_raw.get("duracao"),
                "sinopse": filme_dict_raw.get("sinopse")
            }
            filmes_formatados.append(filme_mapeado)
        
        print(f"DEBUG CASSANDRA: {len(filmes_formatados)} filmes formatados encontrados para o ator.")

        return _ordenar_e_filtrar_resultados_cassandra(filmes_formatados, {}, ordenar_por, ordem, limite)

    except Exception as e:
        # Lembre-se de usar repr(e) se o erro "not all arguments converted" persistir em outros lugares
        raise DatabaseInteractionError(f"Erro ao buscar filmes por ator (Cassandra) para ID '{ator_id_encontrado}': {repr(e)}")


def buscar_atores_por_filmes( # Atores de UM filme específico
    session: Session, 
    id_filme: str,
    atores_tabela: str = "atores", 
    elenco_tabela: str = "elenco", 
    filmes_tabela: str = "filmes" # Embora não usada diretamente para buscar filmes aqui, pode ser útil no futuro
) -> List[Dict[str, Any]]:
    """Busca atores de um filme. Retorna lista de dicts (ator + personagem)."""
    print(f"--- Iniciando buscar_atores_por_filmes (Cassandra) para titulo_id: {id_filme} ---")
    try:
        # 1. Buscar ator_ids e nome_personagem da tabela elenco para o titulo_id_filme
        #   Esta query pode precisar de ALLOW FILTERING se titulo_id não for PK de elenco.
        #   Ou, idealmente, uma tabela denormalizada elenco_por_filme (filme_id PK, ator_id, nome_personagem clustering).
        query_elenco_str = f"SELECT ator_id, nome_personagem FROM {elenco_tabela} WHERE titulo_id = %s ALLOW FILTERING"
        elenco_participacoes = list(session.execute(SimpleStatement(query_elenco_str), (id_filme,)))
        
        if not elenco_participacoes:
            print(f"DEBUG CASSANDRA: Nenhum ator encontrado no elenco para o filme_id: {id_filme}")
            return []

        ator_ids_no_filme = list(set([p.ator_id for p in elenco_participacoes])) # Pega IDs únicos dos atores
        
        if not ator_ids_no_filme: return []

        # 2. Buscar detalhes dos atores
        placeholders_atores = ', '.join(['%s'] * len(ator_ids_no_filme))
        query_atores_str = f"SELECT ator_id, nome_ator, ano_nascimento FROM {atores_tabela} WHERE ator_id IN ({placeholders_atores})"
        atores_info_rows = session.execute(SimpleStatement(query_atores_str), tuple(ator_ids_no_filme))
        map_ator_id_to_info = {ator.ator_id: ator._asdict() for ator in atores_info_rows}

        # 3. Montar o resultado final
        resultado_final = []
        for participacao in elenco_participacoes:
            ator_id = participacao.ator_id
            ator_info = map_ator_id_to_info.get(ator_id)
            if ator_info:
                # O AtorResponse no Pydantic espera 'id' (alias de _id), 'ator_id', 'nome_ator', 'nome_personagem'.
                # Vamos montar o dicionário para ser compatível.
                # "outros_filmes" é muito custoso de buscar aqui no Cassandra sem modelagem específica.
                # Para o TCC, podemos omitir "outros_filmes" ou buscar apenas alguns poucos se muito necessário.
                # Por simplicidade e performance, omitiremos "outros_filmes" nesta versão para Cassandra.
                ator_para_resposta = {
                    "_id": ator_id, # Usando ator_id como 'id' para compatibilidade com AtorResponse
                    "ator_id": ator_id,
                    "nome_ator": ator_info.get("nome_ator"),
                    "ano_nascimento": ator_info.get("ano_nascimento"),
                    "nome_personagem": participacao.nome_personagem
                    # "outros_filmes": [] # Omitido por performance no Cassandra
                }
                resultado_final.append(ator_para_resposta)
        
        print(f"DEBUG CASSANDRA: {len(resultado_final)} atores encontrados para o filme {id_filme}")
        return resultado_final
    except Exception as e:
        raise DatabaseInteractionError(f"Erro ao buscar atores do filme '{id_filme}' (Cassandra): {e}")


# --- ATUALIZAÇÃO ---

def atualizar_campo_filme(session: Session, titulo_id: str, campo_para_atualizar: str, novo_valor: Any, tabela: str = "filmes") -> bool:
    """Atualiza um campo específico de um filme. Levanta ItemNotFoundError."""
    # 1. Verificar se o filme existe
    print(session)
    print(titulo_id)
    print(tabela)
    filme_existente = buscar_filme_por_id(session, titulo_id, tabela)
    if not filme_existente:
        raise ItemNotFoundError(f"Filme com titulo_id '{titulo_id}' não encontrado para atualização no Cassandra.")
    
    # Lista de campos permitidos para atualização (para segurança, se 'campo_para_atualizar' vier de input do usuário)
    # campos_permitidos = ["titulo", "tipo", "ano_lancamento", "generos", "nota", "numero_votos", "duracao", "sinopse"]
    # if campo_para_atualizar not in campos_permitidos:
    #     raise DataValidationError(f"Campo '{campo_para_atualizar}' não é permitido para atualização direta.")

    # Adapta o novo_valor se necessário (ex: para listas)
    if campo_para_atualizar == "generos" and not isinstance(novo_valor, list) and novo_valor is not None:
        # Supondo que pode vir como string separada por vírgulas
        novo_valor = [g.strip() for g in str(novo_valor).split(",")] if str(novo_valor) else []
    elif campo_para_atualizar in ["ano_lancamento", "numero_votos", "duracao"] and novo_valor is not None:
        try: novo_valor = int(novo_valor)
        except ValueError: raise DataValidationError(f"Valor para '{campo_para_atualizar}' deve ser um inteiro.")
    elif campo_para_atualizar == "nota" and novo_valor is not None:
        try: novo_valor = float(novo_valor)
        except ValueError: raise DataValidationError(f"Valor para '{campo_para_atualizar}' deve ser numérico.")

    try:
        query_update_str = f"UPDATE {tabela} SET {campo_para_atualizar} = %s WHERE titulo_id = %s"
        session.execute(SimpleStatement(query_update_str), (novo_valor, titulo_id))
        return True # UPDATE no Cassandra é um upsert e não retorna contagem facilmente. Sucesso se não deu erro.
    except Exception as e:
        raise DatabaseInteractionError(f"Erro ao atualizar campo '{campo_para_atualizar}' do filme '{titulo_id}' (Cassandra): {e}")


# --- REMOÇÃO ---

def remover_filme(session: Session, titulo_id: str, tabela: str = "filmes") -> bool:
    """Remove um filme. Levanta ItemNotFoundError se não encontrado."""
    # 1. Verificar se o filme existe para poder levantar ItemNotFoundError corretamente
    filme_existente = buscar_filme_por_id(session, titulo_id, tabela)
    if not filme_existente:
        raise ItemNotFoundError(f"Filme com titulo_id '{titulo_id}' não encontrado para remoção no Cassandra.")
    
    try:
        query_delete_str = f"DELETE FROM {tabela} WHERE titulo_id = %s"
        session.execute(SimpleStatement(query_delete_str), (titulo_id,))
        # Em Cassandra, se a query não der erro, assume-se sucesso.
        # Para ter 100% de certeza, seria preciso tentar um SELECT depois e verificar se sumiu.
        return True
    except Exception as e:
        raise DatabaseInteractionError(f"Erro ao remover filme '{titulo_id}' (Cassandra): {e}")


# --- AGREGAÇÃO / ANÁLISE ---
# Estas funções no Cassandra geralmente requerem leitura de muitos dados e processamento em Python.
# O uso de `ALLOW FILTERING` é comum, mas deve ser notado quanto à performance em grandes datasets.

def contar_filmes_por_ano(session: Session, filmes_tabela: str = "filmes") -> List[Dict[str, Any]]:
    """Conta filmes por ano de lançamento. Retorna lista de {'_id': ano, 'quantidade': qtd}."""
    try:
        # Lê todos os anos da tabela. CUIDADO: performance em tabelas grandes!
        query = SimpleStatement(f"SELECT ano_lancamento FROM {filmes_tabela} ALLOW FILTERING")
        rows = session.execute(query)
        contagem = {}
        for row in rows:
            ano = row.ano_lancamento
            if ano is not None: # Ignora filmes sem ano de lançamento definido
                contagem[ano] = contagem.get(ano, 0) + 1
        
        # Formata para o padrão esperado pelo serviço (similar ao MongoDB)
        # O alias '_id' para 'ano' é para compatibilidade com o ContagemPorAnoResponse
        return [{"_id": ano, "quantidade": qtd} for ano, qtd in sorted(contagem.items())]
    except Exception as e:
        raise DatabaseInteractionError(f"Erro ao contar filmes por ano (Cassandra): {e}")


def media_notas_por_genero(session: Session, filmes_tabela: str = "filmes") -> List[Dict[str, Any]]:
    """Calcula a média de notas por gênero. Retorna lista de {'genero': nome, 'media_nota': media}."""
    try:
        # Lê todos os gêneros e notas. CUIDADO: performance!
        query = SimpleStatement(f"SELECT generos, nota FROM {filmes_tabela} ALLOW FILTERING") 
        rows = session.execute(query)
        
        generos_notas_soma_contagem: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            # 'generos' é uma lista e 'nota' é float/int
            if isinstance(row.generos, list) and row.nota is not None:
                try:
                    nota_val = float(row.nota) # Garante que nota seja float
                    for genero_item in row.generos:
                        if genero_item not in generos_notas_soma_contagem:
                            generos_notas_soma_contagem[genero_item] = {"soma": 0.0, "contagem": 0}
                        generos_notas_soma_contagem[genero_item]["soma"] += nota_val
                        generos_notas_soma_contagem[genero_item]["contagem"] += 1
                except ValueError:
                    print(f"AVISO CASSANDRA: Nota inválida '{row.nota}' para filme durante cálculo de média por gênero. Pulando.")
                    continue
        
        resultado_final = []
        for genero, dados in generos_notas_soma_contagem.items():
            if dados["contagem"] > 0:
                media = round(dados["soma"] / dados["contagem"], 2)
                resultado_final.append({"genero": genero, "media_nota": media})
            # else: # Opcional: incluir gêneros com contagem 0 e média 0.0
            #     resultado_final.append({"genero": genero, "media_nota": 0.0}) 
        
        # Ordena pelo nome do gênero para consistência, ou pela média_nota se preferir
        resultado_final.sort(key=lambda x: x.get("media_nota", 0.0), reverse=True)
        return resultado_final
    except Exception as e:
        raise DatabaseInteractionError(f"Erro ao calcular média de notas por gênero (Cassandra): {e}")


# --- UTILITÁRIOS DE CARGA DE DADOS --- (Mantendo a sua estrutura original que é boa)

def _limpar_generos_cassandra(valor_generos: Any) -> Optional[List[str]]:
    if valor_generos is None: return None 
    if isinstance(valor_generos, list):
        return [str(g).strip() for g in valor_generos if str(g).strip()]
    if isinstance(valor_generos, str):
        # Remove colchetes e aspas se vierem de uma representação string de lista
        valor_limpo = re.sub(r"[\[\]\'\"]", "", valor_generos) 
        return [g.strip() for g in valor_limpo.split(",") if g.strip()]
    return None

def carregar_dados(
    session: Session, 
    filmes_path: str, 
    atores_path: str, 
    elenco_path: str,
    filmes_tabela: str = "filmes",
    atores_tabela: str = "atores",
    elenco_tabela: str = "elenco"
) -> Dict[str, Any]:
    """Carrega dados dos arquivos TSV para o Cassandra."""
    # ... (sua lógica de TRUNCATE TABLE e preparação de statements permanece a mesma) ...
    # ... (ela já está correta, usando ? como placeholder)
    try:
        session.execute(f"TRUNCATE TABLE {filmes_tabela}")
        session.execute(f"TRUNCATE TABLE {atores_tabela}")
        session.execute(f"TRUNCATE TABLE {elenco_tabela}")
    except Exception as e:
        return {"status": "falha_truncate", "message": f"Falha ao truncar tabelas: {e}"}

    erros_validacao_e_carga = []
    filmes_inseridos_count = 0
    atores_inseridos_count = 0
    elenco_inserido_count = 0

    prepared_stmt_filme = session.prepare(f"INSERT INTO {filmes_tabela} (titulo_id, titulo, tipo, ano_lancamento, generos, nota, numero_votos, duracao, sinopse) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
    prepared_stmt_ator = session.prepare(f"INSERT INTO {atores_tabela} (ator_id, nome_ator, ano_nascimento) VALUES (?, ?, ?)")
    prepared_stmt_elenco = session.prepare(f"INSERT INTO {elenco_tabela} (ator_id, titulo_id, nome_personagem) VALUES (?, ?, ?)")

    # --- AJUSTE NA INSERÇÃO DE FILMES ---
    try:
        filmes_df = pd.read_csv(filmes_path, sep='\t', keep_default_na=False, na_values=['\\N', 'nan', '', 'None'])
        for idx, row in filmes_df.iterrows():
            try:
                # Monta o dicionário para validação Pydantic e para a tupla de inserção
                filme_para_cassandra = {
                    'titulo_id': str(row['titulo_id']) if pd.notna(row['titulo_id']) else None,
                    'titulo': str(row['titulo']) if pd.notna(row['titulo']) else None,
                    'tipo': str(row['tipo']) if pd.notna(row['tipo']) else None,
                    'ano_lancamento': int(row['ano_lancamento']) if pd.notna(row['ano_lancamento']) and str(row['ano_lancamento']).strip() else None,
                    'generos': _limpar_generos_cassandra(row.get('generos')),
                    'nota': float(row['nota']) if pd.notna(row['nota']) and str(row['nota']).strip() else None,
                    'numero_votos': int(row['numero_votos']) if pd.notna(row['numero_votos']) and str(row['numero_votos']).strip() else None,
                    'duracao': int(row['duracao']) if pd.notna(row['duracao']) and str(row['duracao']).strip() else None,
                    'sinopse': str(row['sinopse']) if pd.notna(row['sinopse']) else None
                }
                
                if filme_para_cassandra['titulo_id'] is None:
                    erros_validacao_e_carga.append({"arq": "filmes", "ln": idx+2, "id": "N/A", "err": "Validação", "det": "titulo_id não pode ser nulo."})
                    continue

                # Validação Pydantic: Para validar com o modelo Filme, que agora espera 'id' (alias de _id)
                # e não tem mais 'titulo_id', a gente cria um dicionário temporário para ele.
                dados_para_pydantic_filme = filme_para_cassandra.copy()
                dados_para_pydantic_filme['id'] = dados_para_pydantic_filme.pop('titulo_id') # Mapeia titulo_id para id
                _ = FilmeModelPydantic(**dados_para_pydantic_filme) # Valida

                # Executa o insert com a tupla de valores na ordem correta
                session.execute(prepared_stmt_filme, tuple(filme_para_cassandra.values()))
                filmes_inseridos_count += 1
            except (ValidationError, DataValidationError) as e_val:
                erros_validacao_e_carga.append({"arq": "filmes", "ln": idx+2, "id": row.get('titulo_id'), "err": "Pydantic", "det": str(e_val)})
            except Exception as e_db: 
                erros_validacao_e_carga.append({"arq": "filmes", "ln": idx+2, "id": row.get('titulo_id'), "err": "DB Insert", "det": str(e_db)})
    except Exception as e_read_df:
        erros_validacao_e_carga.append({"arq": "filmes", "tipo": "LeituraDF", "det": str(e_read_df)})

    # --- AJUSTE NA INSERÇÃO DE ATORES ---
    try:
        atores_df = pd.read_csv(atores_path, sep='\t', keep_default_na=False, na_values=['\\N', 'nan', '', 'None'])
        for idx, row in atores_df.iterrows():
            try:
                ator_para_cassandra = {
                    'ator_id': str(row['ator_id']) if pd.notna(row['ator_id']) else None,
                    'nome_ator': str(row['nome_ator']) if pd.notna(row['nome_ator']) else None,
                    'ano_nascimento': int(row['ano_nascimento']) if pd.notna(row['ano_nascimento']) and str(row['ano_nascimento']).strip() else None
                }

                if ator_para_cassandra['ator_id'] is None:
                     erros_validacao_e_carga.append({"arq": "atores", "ln": idx+2, "id": "N/A", "err": "Validação", "det": "ator_id não pode ser nulo."})
                     continue
                
                # Validação Pydantic: Mapeia ator_id para id para o modelo Ator
                dados_para_pydantic_ator = ator_para_cassandra.copy()
                dados_para_pydantic_ator['id'] = dados_para_pydantic_ator.pop('ator_id')
                _ = AtorModelPydantic(**dados_para_pydantic_ator)
                
                session.execute(prepared_stmt_ator, tuple(ator_para_cassandra.values()))
                atores_inseridos_count += 1
            except (ValidationError, DataValidationError) as e_val:
                erros_validacao_e_carga.append({"arq": "atores", "ln": idx+2, "id": row.get('ator_id'), "err": "Pydantic", "det": str(e_val)})
            except Exception as e_db:
                erros_validacao_e_carga.append({"arq": "atores", "ln": idx+2, "id": row.get('ator_id'), "err": "DB Insert", "det": str(e_db)})
    except Exception as e_read_df:
        erros_validacao_e_carga.append({"arq": "atores", "tipo": "LeituraDF", "det": str(e_read_df)})

    # --- INSERÇÃO DE ELENCO (JÁ ESTÁ OK, POIS USA AS CHAVES ESTRANGEIRAS) ---
    try:
        elenco_df = pd.read_csv(elenco_path, sep='\t', keep_default_na=False, na_values=['\\N', 'nan', '', 'None'])
        for idx, row in elenco_df.iterrows():
            try:
                elenco_data_dict = {
                    'ator_id': str(row['ator_id']) if pd.notna(row['ator_id']) else None,
                    'titulo_id': str(row['titulo_id']) if pd.notna(row['titulo_id']) else None,
                    'nome_personagem': str(row['nome_personagem']) if pd.notna(row['nome_personagem']) else None 
                }
                _ = ElencoModelPydantic(**elenco_data_dict) # O modelo de elenco já espera ator_id e titulo_id

                if not all([elenco_data_dict['ator_id'], elenco_data_dict['titulo_id'], elenco_data_dict['nome_personagem'] is not None]):
                    erros_validacao_e_carga.append(...)
                    continue

                session.execute(prepared_stmt_elenco, tuple(elenco_data_dict.values()))
                elenco_inserido_count += 1
            except (ValidationError, DataValidationError) as e_val:
                erros_validacao_e_carga.append(...)
            except Exception as e_db:
                erros_validacao_e_carga.append(...)
    except Exception as e_read_df:
        erros_validacao_e_carga.append(...)
    
    # --- MENSAGEM FINAL E RETORNO (PERMANECE IGUAL) ---
    msg = f"Carga Cassandra: {filmes_inseridos_count} filmes, {atores_inseridos_count} atores, {elenco_inserido_count} relações."
    # ... (lógica de status_op e retorno do dicionário completo) ...
    status_op = "concluído_com_erros" if erros_validacao_e_carga else "sucesso"
    if erros_validacao_e_carga: print(f"LOG DE CARGA CASSANDRA - Erros: {erros_validacao_e_carga[:5]}")
    return {
        "status": status_op, "message": msg,
        "detalhes_carga": {"filmes_inseridos": filmes_inseridos_count, "atores_inseridos": atores_inseridos_count, "elenco_inserido": elenco_inserido_count},
        "erros_execucao": erros_validacao_e_carga
    }