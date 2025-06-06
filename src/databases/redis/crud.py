# src/databases/redis/crud.py
import redis, pandas as pd
import json # Necessário para _serialize_redis_value se lidar com dict/list
import re   # Necessário para _limpar_generos_redis
from typing import List, Dict, Any, Optional
from src.models.filme import Filme # Importa seu modelo de Filme
from src.models.ator import Ator # Importa seu modelo de Ator
from src.models.elenco import Elenco # Importa seu modelo de Elenco

# from pydantic import ValidationError # Se não estiver usando aqui.

from src.core.exceptions import (
    DataValidationError, DatabaseInteractionError,
    ItemNotFoundError, ItemAlreadyExistsError)

# --- PREFIXOS DE CHAVE (COMO VOCÊ DEFINIU) ---
FILME_KEY_PREFIX = "filme:"
ATOR_KEY_PREFIX = "ator:"
# Relações Elenco
ELENCO_FILME_ATORES_PREFIX = "elenco:filme_atores:" # Usado em inserir_elenco e buscar_atores_por_filmes
ELENCO_ATOR_FILMES_PREFIX = "elenco:ator_filmes:"   # Usado em inserir_elenco e buscar_filmes_por_ator
PERSONAGEM_PROPS_KEY_PREFIX = "personagem_props:"   # Usado para nome_personagem
# Índices Secundários
IDX_FILME_ANO_PREFIX = "idx:filme:ano:"
IDX_FILME_GENERO_PREFIX = "idx:filme:genero:"
IDX_FILME_TIPO_PREFIX = "idx:filme:tipo:"

# --- FUNÇÕES AUXILIARES (COMO VOCÊ JÁ TEM OU PRECISA) ---
def _limpar_generos_redis(valor_generos: Any) -> List[str]:
    if not valor_generos: return []
    if isinstance(valor_generos, list): return [str(g).strip() for g in valor_generos if str(g).strip()]
    if isinstance(valor_generos, str):
        valor_limpo = re.sub(r"[\[\]\'\"]", "", valor_generos)
        return [g.strip() for g in valor_limpo.split(",") if g.strip()]
    return []

def _serialize_redis_value(value: Any) -> str:
    if isinstance(value, (list, dict)): return json.dumps(value)
    if value is None: return ""
    return str(value)

def _deserialize_redis_filme(filme_hash: Dict[str, str]) -> Optional[Dict[str, Any]]:
    if not filme_hash: return None
    filme = {}
    for key, value_str in filme_hash.items():
        if not value_str: 
            filme[key] = None
            continue
        if key == "generos":
            try: filme[key] = json.loads(value_str)
            except json.JSONDecodeError: filme[key] = []
        elif key in ["ano_lancamento", "numero_votos", "duracao"]:
            try: filme[key] = int(value_str)
            except ValueError: filme[key] = None
        elif key == "nota":
            try: filme[key] = float(value_str)
            except ValueError: filme[key] = None
        else: # Inclui _id, titulo_id, titulo, tipo, sinopse
            filme[key] = value_str
    # Garante _id e titulo_id se um deles existir (já feito no seu código)
    if "titulo_id" in filme and "_id" not in filme: filme["_id"] = filme["titulo_id"]
    elif "_id" in filme and "titulo_id" not in filme: filme["titulo_id"] = filme["_id"]
    return filme

def _deserialize_redis_ator(ator_hash: Dict[str, str]) -> Optional[Dict[str, Any]]:
    if not ator_hash: return None
    ator = {}
    for key, value_str in ator_hash.items():
        if not value_str: ator[key] = None; continue
        if key == "ano_nascimento":
            try: ator[key] = int(value_str)
            except ValueError: ator[key] = None
        else: # Inclui _id, ator_id, nome_ator
            ator[key] = value_str
    if "ator_id" in ator and "_id" not in ator: ator["_id"] = ator["ator_id"]
    elif "_id" in ator and "ator_id" not in ator: ator["ator_id"] = ator["_id"]
    return ator

# src/databases/redis/crud.py
# ... (imports e outras funções auxiliares) ...

def inserir_filme(r: redis.Redis, filme_data: Dict[str, Any]) -> Dict[str, Any]:
    """Insere um filme no Redis. Usa _id ou titulo_id como fonte para a chave e padroniza para _id."""

    dados_para_salvar = filme_data.copy()

    # --- LÓGICA DE ID MAIS ROBUSTA ---
    # Pega o ID da chave '_id' ou, se não houver, da chave 'titulo_id'.
    filme_id_val = dados_para_salvar.get("_id") or dados_para_salvar.get("titulo_id")
    if not filme_id_val:
        raise DataValidationError("ID do filme (_id ou titulo_id) é obrigatório para inserir no Redis.")
    
    filme_id = str(filme_id_val)

    # Garante a padronização: o dicionário final terá a chave '_id'
    dados_para_salvar["_id"] = filme_id
    # E remove a chave 'titulo_id' se ela existir, para evitar redundância
    if "titulo_id" in dados_para_salvar:
        dados_para_salvar.pop("titulo_id")
    # --- FIM DA LÓGICA DE ID ---

    chave_filme = f"{FILME_KEY_PREFIX}{filme_id}"

    # 1. Verifica se já existe
    if r.exists(chave_filme):
        raise ItemAlreadyExistsError(f"Filme com ID '{filme_id}' já existe no Redis.")
    
    # 2. Prepara e insere o payload já limpo
    payload_redis = {k: _serialize_redis_value(v) for k, v in dados_para_salvar.items() if v is not None}
    
    try:
        r.hmset(chave_filme, payload_redis)
        
        # 3. Atualizar índices secundários (agora usando 'dados_para_salvar')
        if "generos" in dados_para_salvar and dados_para_salvar["generos"]:
            for genero in dados_para_salvar["generos"]:
                if genero and str(genero).strip():
                    r.sadd(f"{IDX_FILME_GENERO_PREFIX}{_serialize_redis_value(str(genero).strip())}", filme_id)
        
        if "ano_lancamento" in dados_para_salvar and dados_para_salvar["ano_lancamento"] is not None:
            r.sadd(f"{IDX_FILME_ANO_PREFIX}{dados_para_salvar['ano_lancamento']}", filme_id)
        
        if "tipo" in dados_para_salvar and dados_para_salvar["tipo"]:
            if str(dados_para_salvar["tipo"]).strip():
                 r.sadd(f"{IDX_FILME_TIPO_PREFIX}{_serialize_redis_value(str(dados_para_salvar['tipo']).strip())}", filme_id)
        
        # Retorna o dicionário limpo e padronizado
        return dados_para_salvar
    except Exception as e:
        raise DatabaseInteractionError(f"Erro ao inserir novo filme '{filme_id}' no Redis: {e}")


def inserir_ator(r: redis.Redis, ator_data: Dict[str, Any]) -> Dict[str, Any]:
    """Insere um ator no Redis. Usa _id ou ator_id como fonte para a chave e padroniza para _id."""
    
    dados_para_salvar = ator_data.copy()

    ator_id_val = dados_para_salvar.get("_id") or dados_para_salvar.get("ator_id")
    if not ator_id_val:
        raise DataValidationError("ID do ator (_id ou ator_id) é obrigatório para inserir no Redis.")
        
    ator_id = str(ator_id_val)

    dados_para_salvar["_id"] = ator_id
    if "ator_id" in dados_para_salvar:
        dados_para_salvar.pop("ator_id")
    
    chave_ator = f"{ATOR_KEY_PREFIX}{ator_id}"

    if r.exists(chave_ator):
        raise ItemAlreadyExistsError(f"Ator com ID '{ator_id}' já existe no Redis.")
    
    payload_redis = {k: _serialize_redis_value(v) for k, v in dados_para_salvar.items() if v is not None}
    
    try:
        r.hmset(chave_ator, payload_redis)
        # Lógica para indexar atores aqui, se necessário
        return dados_para_salvar
    except Exception as e:
        raise DatabaseInteractionError(f"Erro ao inserir novo ator '{ator_id}' no Redis: {e}")
    

def inserir_elenco(r: redis.Redis, elenco_data: Dict[str, Any]) -> bool:
    """Adiciona relações de elenco usando Sets no Redis."""
    # Lembre-se que no Redis, o _id de ator e filme são seus ator_id e titulo_id
    ator_id = str(elenco_data.get("ator_id"))    # Que é o _id do Ator no Redis
    titulo_id = str(elenco_data.get("titulo_id")) # Que é o _id do Filme no Redis
    nome_personagem = elenco_data.get("nome_personagem")

    if not ator_id or not titulo_id:
        raise DataValidationError("ator_id e titulo_id são obrigatórios para inserir elenco no Redis.")

    try:
        # Cria uma pipeline para executar os comandos de forma atômica (ou o mais próximo disso no Redis)
        pipe = r.pipeline()
        
        # Chave para o set de atores de um filme: "elenco:filme_atores:tt123456"
        chave_filme_com_seus_atores = f"{ELENCO_FILME_ATORES_PREFIX}{titulo_id}"
        pipe.sadd(chave_filme_com_seus_atores, ator_id)
        
        # Chave para o set de filmes de um ator: "elenco:ator_filmes:nm123456"
        chave_ator_com_seus_filmes = f"{ELENCO_ATOR_FILMES_PREFIX}{ator_id}"
        pipe.sadd(chave_ator_com_seus_filmes, titulo_id)
        
        # Opcional: Armazenar propriedades da relação (como nome_personagem)
        # Se nome_personagem for importante e único para a relação ator-filme.
        # Uma forma é usar um Hash cuja chave é uma combinação de filme_id e ator_id.
        # Dentro de inserir_elenco, quando nome_personagem is not None:
        if nome_personagem is not None:
            chave_propriedades_relacao = f"{PERSONAGEM_PROPS_KEY_PREFIX}{titulo_id}:{ator_id}"
            pipe.hset(chave_propriedades_relacao, "nome", _serialize_redis_value(nome_personagem))
            # chave_propriedades_relacao = f"{RELACAO_PROPS_PREFIX}{titulo_id}:{ator_id}"
            # Se um ator pode ter múltiplos personagens no MESMO filme, essa chave acima não é suficiente.
            # Nesse caso, a modelagem do elenco no Redis precisaria ser mais complexa
            # ou você assumiria que 'nome_personagem' é uma propriedade da relação ator-filme e pode ser sobrescrita
            # ou o nome_personagem faria parte da chave do set (o que é incomum).
            # Por simplicidade, se for apenas UMA relação principal ator-filme, podemos adicionar
            # o nome_personagem a um hash separado ou como parte de um score em um sorted set se a ordem importasse.
            # Exemplo simples (sobrescreve se já existir para o par ator-filme):
            # r.hset(f"personagem:{titulo_id}:{ator_id}", "nome", _serialize_redis_value(nome_personagem))
            pass # Decida como/se quer armazenar nome_personagem de forma granular

        pipe.execute() # Executa os comandos SADD na pipeline
        return True
    except Exception as e:
        raise DatabaseInteractionError(f"Erro ao inserir relação de elenco para ator '{ator_id}' e filme '{titulo_id}' no Redis: {e}")

# --- CONSULTAS ---
def buscar_filme_por_id(r: redis.Redis, id_filme: str) -> Optional[Dict[str, Any]]:
    chave_filme = f"{FILME_KEY_PREFIX}{str(id_filme)}"
    try:
        filme_hash = r.hgetall(chave_filme)
        if not filme_hash:
            raise ItemNotFoundError(f"Filme com ID '{id_filme}' não encontrado no Redis.")
        # Converte chaves e valores de bytes para string
        return _deserialize_redis_filme(filme_hash)
    except ItemNotFoundError:
        raise
    except Exception as e:
        raise DatabaseInteractionError(f"Erro ao buscar filme ID '{id_filme}' no Redis: {e}")

# (buscar_ator_por_id similar)
def buscar_ator_por_id(r: redis.Redis, id_ator: str) -> Optional[Dict[str, Any]]:
    chave_ator = f"{ATOR_KEY_PREFIX}{str(id_ator)}"
    try:
        ator_hash = r.hgetall(chave_ator)
        if not ator_hash:
            raise ItemNotFoundError(f"Ator com ID '{id_ator}' não encontrado no Redis.")
        return _deserialize_redis_ator(ator_hash)
    except ItemNotFoundError:
        raise
    except Exception as e:
        raise DatabaseInteractionError(f"Erro ao buscar ator ID '{id_ator}' no Redis: {e}")

# --- NOVA FUNÇÃO AUXILIAR DE FILTRAGEM PYTHON PARA REDIS ---
def _aplicar_filtros_python_redis(
    filme: Dict[str, Any],
    filtros_aplicar: Dict[str, Any], # Dicionário com os filtros a serem checados
    ano_corte_futuro: int
) -> bool:
    """
    Aplica filtros Python a um filme (já recuperado do Redis),
    considerando a regra para filmes futuros.
    """
    if not filtros_aplicar: # Se não há filtros para aplicar em Python, o filme passa
        return True

    match = True
    ano_lancamento_filme = filme.get("ano_lancamento") # Já deve ser int ou None
    nota_filme = filme.get("nota") # Já deve ser float ou None
    votos_filme = filme.get("numero_votos") # Já deve ser int ou None

    is_filme_futuro_sem_avaliacao = False
    if ano_lancamento_filme is not None and ano_lancamento_filme >= ano_corte_futuro:
        if (nota_filme is None or nota_filme == 0 or nota_filme == 0.0) and \
           (votos_filme is None or votos_filme == 0):
            is_filme_futuro_sem_avaliacao = True
            print(f"DEBUG FILTRO REDIS: Filme '{filme.get('_id')}' (ano: {ano_lancamento_filme}) é futuro sem avaliação.") #DEBUG

    for campo_filtro, valor_filtro in filtros_aplicar.items():
        # Pula filtros com valor None ou listas de gênero vazias, pois não filtram nada.
        if valor_filtro is None or (isinstance(valor_filtro, list) and not valor_filtro):
            continue

        # Filtros que sempre se aplicam
        if campo_filtro == "titulo_contem": # titulo já é string
            if valor_filtro.lower() not in filme.get("titulo", "").lower():
                match = False; break
        elif campo_filtro == "tipo": # tipo já é string. Este filtro pode ser redundante se já usado no índice.
            if filme.get("tipo", "").lower() != valor_filtro.lower():
                match = False; break
        elif campo_filtro == "generos_contem_todos": # filme.get("generos") já é list[str]
            # Este filtro é importante se múltiplos gêneros foram passados e o índice só usou um (ou nenhum).
            filme_generos_set = set(g.lower() for g in filme.get("generos", []))
            filtro_generos_set = set(g.lower() for g in valor_filtro) # valor_filtro é a lista de gêneros do filtro
            if not filtro_generos_set.issubset(filme_generos_set):
                match = False; break
        elif campo_filtro == "ano_lancamento_min": # ano_lancamento_filme já é int ou None
            if ano_lancamento_filme is None or ano_lancamento_filme < valor_filtro:
                match = False; break
        
        # Filtros de "performance" que são ignorados para filmes futuros sem avaliação
        elif campo_filtro in ["nota_min", "duracao_min"]: # Adicione "numero_votos_min" se for implementar
            if is_filme_futuro_sem_avaliacao:
                print(f"DEBUG FILTRO REDIS: Pulando filtro '{campo_filtro}' para filme futuro sem avaliação '{filme.get('_id')}'.") #DEBUG
                continue 

            if campo_filtro == "nota_min": # nota_filme já é float ou None
                if nota_filme is None or nota_filme < valor_filtro:
                    match = False; break
            elif campo_filtro == "duracao_min": # filme.get("duracao") já é int ou None
                tipo_filme_atual = filme.get("tipo", "").lower()
                if tipo_filme_atual != "jogo": 
                    if filme.get("duracao") is None or filme.get("duracao") < valor_filtro:
                        match = False; break
        # else:
        #     print(f"DEBUG FILTRO REDIS: Filtro Python não reconhecido ou não aplicável: '{campo_filtro}'")

    # if not match: print(f"DEBUG FILTRO REDIS: Filme '{filme.get('_id')}' REPROVADO pelos filtros Python.") #DEBUG
    return match

# --- FUNÇÃO buscar_filmes_avancado ATUALIZADA ---
def buscar_filmes_avancado(
    r: redis.Redis,
    titulo: Optional[str] = None, 
    tipo: Optional[str] = None, # Parâmetro vindo do serviço
    ano_min: Optional[int] = None, # Parâmetro vindo do serviço (era ano_lancamento_min no payload)
    generos: Optional[List[str]] = None, # Parâmetro vindo do serviço
    nota_min: Optional[float] = None, # Parâmetro vindo do serviço
    duracao_min: Optional[int] = None, # Parâmetro vindo do serviço
    ordenar_por: str = "nota", 
    ordem: int = -1,
    limite: Optional[int] = 10000, 
    ano_corte_futuro: int = 2025 # Parâmetro vindo do serviço
) -> List[Dict[str, Any]]:
    
    print("INFO REDIS (Avançada): Iniciando busca avançada. Nota: Pode ser ineficiente sem RediSearch para múltiplos filtros complexos.")
    
    ids_candidatos_set: Optional[set] = set() # Inicia como set vazio para SINTER
    usou_indice_primario = False

    # Tenta usar o índice mais seletivo primeiro ou uma combinação.
    # Exemplo: se 'tipo' for fornecido, usa como filtro primário.
    # Se 'generos' (único) for fornecido, usa.
    # Se ambos, pode fazer SINTER.
    
    chaves_indices_para_intersecao = []
    if tipo:
        chave_idx_tipo = f"{IDX_FILME_TIPO_PREFIX}{_serialize_redis_value(tipo)}"
        chaves_indices_para_intersecao.append(chave_idx_tipo)
        print(f"DEBUG REDIS (Avançada): Usando índice de TIPO: {chave_idx_tipo}")
        usou_indice_primario = True # Marca que pelo menos um índice foi usado

    if generos and len(generos) == 1: # Se UM gênero específico foi fornecido
        chave_idx_genero = f"{IDX_FILME_GENERO_PREFIX}{_serialize_redis_value(generos[0])}"
        chaves_indices_para_intersecao.append(chave_idx_genero)
        print(f"DEBUG REDIS (Avançada): Usando índice de GÊNERO único: {chave_idx_genero}")
        usou_indice_primario = True

    # Adicione outros índices aqui se tiver (ex: ano_min poderia ser um range com ZSET, mas é mais complexo)

    if chaves_indices_para_intersecao:
        if len(chaves_indices_para_intersecao) > 1:
            print(f"DEBUG REDIS (Avançada): Executando SINTER em chaves: {chaves_indices_para_intersecao}")
            ids_candidatos_set = r.sinter(chaves_indices_para_intersecao) # Retorna set de strings
        else:
            ids_candidatos_set = r.smembers(chaves_indices_para_intersecao[0]) # Retorna set de strings
        print(f"DEBUG REDIS (Avançada): IDs candidatos via SINTER/SMEMBERS: {len(ids_candidatos_set) if ids_candidatos_set else 0}")
    
    ids_candidatos_str_list: List[str]

    if not usou_indice_primario: # Se nenhum índice primário foi usado (tipo ou genero único)
        print("AVISO REDIS (Avançada): Nenhum índice primário utilizado. Recorrendo a SCAN de todas as chaves de filme. Isso PODE ser LENTO!")
        chaves_filmes_com_prefixo = list(r.scan_iter(match=f"{FILME_KEY_PREFIX}*")) # Já são strings
        ids_candidatos_str_list = [key_str.split(':', 1)[1] for key_str in chaves_filmes_com_prefixo if ':' in key_str]
        print(f"DEBUG REDIS (Avançada): IDs obtidos via SCAN: {len(ids_candidatos_str_list)}")
    else:
        ids_candidatos_str_list = list(ids_candidatos_set if ids_candidatos_set is not None else []) # Converte set para lista
        if not ids_candidatos_str_list and chaves_indices_para_intersecao: # Se usou índice mas SINTER deu vazio
             print(f"INFO REDIS (Avançada): Interseção de índices resultou em zero IDs. Retornando lista vazia.")
             return []


    resultados_brutos = []
    print(f"DEBUG REDIS (Avançada): Buscando detalhes para até {len(ids_candidatos_str_list)} IDs candidatos...")
    for filme_id_str in ids_candidatos_str_list:
        try:
            # buscar_filme_por_id já desserializa os campos corretamente
            filme_dict = buscar_filme_por_id(r, filme_id_str) 
            if filme_dict: 
                resultados_brutos.append(filme_dict)
        except ItemNotFoundError: 
            print(f"DEBUG REDIS (Avançada): Filme ID '{filme_id_str}' (de índice/scan) não encontrado ao buscar detalhes. Pulando.")
            continue
        except Exception as e_busca_detalhe:
            print(f"DEBUG REDIS (Avançada): Erro ao buscar detalhes do filme_id '{filme_id_str}': {e_busca_detalhe}")
            continue
    
    print(f"DEBUG REDIS (Avançada): {len(resultados_brutos)} filmes brutos recuperados antes da filtragem Python.")

    # Preparar o dicionário de filtros para a função _aplicar_filtros_python_redis
    # Os nomes das chaves aqui devem bater com os esperados por _aplicar_filtros_python_redis
    filtros_para_python = {
        "titulo_contem": titulo,
        # Se 'tipo' não foi usado como índice primário OU se você quer uma checagem dupla:
        "tipo": tipo if not (usou_indice_primario and tipo and chaves_indices_para_intersecao and f"{IDX_FILME_TIPO_PREFIX}{_serialize_redis_value(tipo)}" in chaves_indices_para_intersecao) else None,
        "ano_lancamento_min": ano_min,
        # Se 'generos' (único) não foi usado OU se a lista de generos tem mais de um (precisa de $all)
        "generos_contem_todos": generos if not (usou_indice_primario and generos and len(generos)==1 and chaves_indices_para_intersecao and f"{IDX_FILME_GENERO_PREFIX}{_serialize_redis_value(generos[0])}" in chaves_indices_para_intersecao) or (generos and len(generos) > 1) else None,
        "nota_min": nota_min,
        "duracao_min": duracao_min,
    }
    # Limpa Nones, exceto para generos_contem_todos que pode ser lista vazia se não for filtrar
    filtros_para_python_limpos = {k: v for k, v in filtros_para_python.items() if v is not None or (k == "generos_contem_todos" and isinstance(v, list))}
    print(f"DEBUG REDIS (Avançada): Filtros que serão aplicados em Python: {filtros_para_python_limpos}")

    resultados_filtrados_py = []
    if not resultados_brutos: # Se não tem nada pra filtrar, retorna vazio
        print(f"DEBUG REDIS (Avançada): Nenhum resultado bruto para filtrar em Python.")
    elif not filtros_para_python_limpos: # Se não há filtros Python a aplicar
        print(f"DEBUG REDIS (Avançada): Nenhum filtro Python a aplicar. Usando todos os {len(resultados_brutos)} resultados brutos.")
        resultados_filtrados_py = resultados_brutos
    else:
        for filme_item in resultados_brutos:
            if _aplicar_filtros_python_redis(filme_item, filtros_para_python_limpos, ano_corte_futuro):
                resultados_filtrados_py.append(filme_item)
    
    print(f"DEBUG REDIS (Avançada): {len(resultados_filtrados_py)} filmes após filtragem Python.")
            
    # Ordenação
    if ordenar_por and resultados_filtrados_py:
        def sort_key_redis(item_para_ordenar):
            val = item_para_ordenar.get(ordenar_por) # ordenar_por é string
            default_numeric = float('-inf') if ordem == 1 else float('inf')
            default_str = ""

            if val is None:
                return default_numeric if ordenar_por in ["nota", "ano_lancamento", "numero_votos", "duracao"] else default_str
            
            # Tenta converter para float para campos numéricos conhecidos
            if ordenar_por in ["nota", "ano_lancamento", "numero_votos", "duracao"]:
                try: return float(val)
                except (ValueError, TypeError): return default_numeric # Se falhar, trata como "menor" ou "maior"
            
            # Para outros campos ou se a conversão numérica falhou, trata como string
            return str(val).lower()

        try:
            resultados_filtrados_py.sort(key=sort_key_redis, reverse=(ordem == -1))
            print(f"DEBUG REDIS (Avançada): Ordenação por '{ordenar_por}' (ordem: {ordem}) concluída.")
        except TypeError as te:
            print(f"AVISO REDIS (Avançada): TypeError durante ordenação para '{ordenar_por}': {te}. A lista pode não estar ordenada como esperado.")
    else:
        print(f"DEBUG REDIS (Avançada): Sem ordenação a aplicar ou lista vazia.")

    # Limite final
    limite_final_int = len(resultados_filtrados_py) # Default é pegar todos os filtrados
    if limite is not None and limite >= 0: # Se um limite válido foi fornecido
        limite_final_int = limite
    
    filmes_finais = resultados_filtrados_py[:limite_final_int]
    print(f"DEBUG REDIS (Avançada): Retornando {len(filmes_finais)} filmes dos {len(resultados_filtrados_py)} filtrados (limite aplicado: {limite_final_int}).")
    return filmes_finais

# ... (suas outras funções como buscar_filmes_por_ator, buscar_atores_por_filme, contagem_por_ano, etc.)
# Lembre-se de que a função `buscar_filmes_por_ator` no Redis também precisa da lógica de ordenação
# que estava como 'pass' anteriormente, similar à que foi adicionada aqui.

def buscar_filmes_por_ator(r: redis.Redis, id_ator: str, ordenar_por: str = 'ano_lancamento', ordem: int = -1, limite: Optional[int] = 10000) -> List[Dict[str, Any]]:
    # USA O MESMO PREFIXO DA FUNÇÃO inserir_elenco
    chave_ator_filmes = f"{ELENCO_ATOR_FILMES_PREFIX}{str(id_ator)}" 
    print(f"DEBUG REDIS: Buscando filmes para o ator '{id_ator}' usando a chave: {chave_ator_filmes}") # DEBUG

    try:
        # r.smembers já retorna um set de strings devido a decode_responses=True
        filme_ids_strings = r.smembers(chave_ator_filmes) 
        
        print(f"DEBUG REDIS: IDs de filmes encontrados no set para o ator '{id_ator}': {filme_ids_strings}") # DEBUG

        if not filme_ids_strings: 
            print(f"DEBUG REDIS: Nenhum ID de filme encontrado para o ator '{id_ator}'. Retornando lista vazia.") # DEBUG
            return []
        
        filmes_do_ator = []
        for filme_id_str in filme_ids_strings: # filme_id_str já é uma string
            # Não precisa mais de .decode()
            try:
                filme_data = buscar_filme_por_id(r, filme_id_str) # buscar_filme_por_id espera uma string
                if filme_data: 
                    filmes_do_ator.append(filme_data)
                else:
                    print(f"DEBUG REDIS: buscar_filme_por_id retornou None para filme_id: {filme_id_str}") # DEBUG
            except ItemNotFoundError:
                print(f"DEBUG REDIS: ItemNotFoundError para filme_id: {filme_id_str}. Continuando...") # DEBUG
                continue 
            except Exception as e_busca_filme: # Pega outros erros na busca individual
                print(f"DEBUG REDIS: Erro ao buscar detalhes do filme_id '{filme_id_str}': {e_busca_filme}")
                continue
        
        print(f"DEBUG REDIS: Número de filmes recuperados para o ator '{id_ator}' antes da ordenação/limite: {len(filmes_do_ator)}") # DEBUG

        # Lembre-se de implementar a lógica de ordenação aqui se precisar!
        # Atualmente está como 'pass'. Seus filmes não serão ordenados.
        if ordenar_por and filmes_do_ator:
            print(f"DEBUG REDIS: Aplicando ordenação por '{ordenar_por}', ordem '{ordem}'...") # DEBUG
            # Substitua 'pass' pela sua função de ordenação, ex:
            # filmes_do_ator.sort(key=lambda x: (x.get(ordenar_por, 0) is None, x.get(ordenar_por, 0)), reverse=(ordem == -1))
            # (A chave de ordenação acima lida com Nones, colocando-os no início se crescente, ou fim se decrescente)
            # Ou use a sua `sort_key_redis` que você mencionou antes.
            def sort_key_redis(item_para_ordenar):
                val = item_para_ordenar.get(ordenar_por)
                default_value_numeric = float('-inf') if ordem == 1 else float('inf')
                default_value_str = ""

                if val is None:
                    # Heurística para tipo de ordenação: se ordenar_por é campo numérico conhecido
                    if ordenar_por in ["nota", "ano_lancamento", "numero_votos", "duracao"]:
                        return default_value_numeric
                    return default_value_str # Para strings ou outros

                if isinstance(val, (int, float)):
                    return float(val)
                elif isinstance(val, str):
                    # Tenta converter para float se for string de número, senão compara como string
                    try: return float(val)
                    except ValueError: return val.lower() # Ordenação de string case-insensitive
                return str(val).lower() # Fallback para outros tipos

            try:
                filmes_do_ator.sort(key=sort_key_redis, reverse=(ordem == -1))
                print(f"DEBUG REDIS: Ordenação concluída.")
            except TypeError as te:
                print(f"AVISO REDIS: TypeError durante ordenação para o campo '{ordenar_por}': {te}. Lista pode não estar ordenada como esperado.")


        final_limit = limite if limite is not None else len(filmes_do_ator)
        filmes_finais = filmes_do_ator[:final_limit]
        print(f"DEBUG REDIS: Retornando {len(filmes_finais)} filmes para o ator '{id_ator}' após limite.") # DEBUG
        return filmes_finais
        
    except AttributeError as ae: # Pega especificamente o erro de 'str' object has no attribute 'decode'
        print(f"ERRO DE CODIFICAÇÃO NO REDIS ao buscar filmes do ator '{id_ator}': {ae}") # DEBUG
        # Isso não deveria mais acontecer após a correção, mas é um bom catch
        raise DatabaseInteractionError(f"Erro de atributo (provavelmente decode) ao buscar filmes do ator '{id_ator}' no Redis: {ae}")
    except Exception as e:
        print(f"ERRO GERAL NO REDIS ao buscar filmes do ator '{id_ator}': {e}") # DEBUG
        # import traceback
        # traceback.print_exc() # Descomente para ver o traceback completo no log do FastAPI
        raise DatabaseInteractionError(f"Erro ao buscar filmes do ator '{id_ator}' no Redis: {e}")

def buscar_atores_por_filmes(r: redis.Redis, id_filme: str, limite: Optional[int] = 10000) -> List[Dict[str, Any]]:
    # USA O MESMO PREFIXO DA FUNÇÃO inserir_elenco para filme->atores
    chave_filme_atores = f"{ELENCO_FILME_ATORES_PREFIX}{str(id_filme)}" 
    print(f"DEBUG REDIS: Buscando atores para o filme '{id_filme}' usando a chave: {chave_filme_atores}") # DEBUG

    try:
        # r.smembers já retorna um set de strings devido a decode_responses=True
        ator_ids_strings = r.smembers(chave_filme_atores)
        print(f"DEBUG REDIS: IDs de atores encontrados no set para o filme '{id_filme}': {ator_ids_strings}") # DEBUG

        if not ator_ids_strings:
            print(f"DEBUG REDIS: Nenhum ID de ator encontrado para o filme '{id_filme}'.") #DEBUG
            return []

        atores_do_filme_com_personagem = []
        for ator_id_str in ator_ids_strings: # ator_id_str já é uma string
            # Não precisa mais de .decode()
            try:
                ator_data = buscar_ator_por_id(r, ator_id_str) # buscar_ator_por_id espera uma string
                if ator_data:
                    # Buscar nome_personagem
                    chave_props_personagem = f"{PERSONAGEM_PROPS_KEY_PREFIX}{id_filme}:{ator_id_str}"
                    nome_personagem_val = r.hget(chave_props_personagem, "nome")
                    
                    # Adiciona nome_personagem ao ator_data (já será string se decode_responses=True)
                    ator_data["nome_personagem"] = nome_personagem_val if nome_personagem_val else "Desconhecido"
                    
                    # Omitindo busca de "outros filmes" por simplicidade no Redis,
                    # mas se precisar, essa lógica entraria aqui.
                    atores_do_filme_com_personagem.append(ator_data)
                else:
                    print(f"DEBUG REDIS: buscar_ator_por_id retornou None para ator_id: {ator_id_str}") # DEBUG
            except ItemNotFoundError:
                print(f"DEBUG REDIS: ItemNotFoundError para ator_id: {ator_id_str} ao buscar detalhes. Continuando...") # DEBUG
                continue
            except Exception as e_busca_ator:
                print(f"DEBUG REDIS: Erro ao buscar detalhes do ator_id '{ator_id_str}': {e_busca_ator}")
                continue
        
        print(f"DEBUG REDIS: Atores com personagem recuperados para o filme '{id_filme}': {len(atores_do_filme_com_personagem)}") # DEBUG

        # Limite (aplicado após buscar todos, pois não há ordenação fácil antes)
        final_limit = limite if limite is not None and limite >= 0 else len(atores_do_filme_com_personagem)
        
        # Se precisar de ordenação aqui (ex: por nome_ator), adicione a lógica de sort.
        # Ex: atores_do_filme_com_personagem.sort(key=lambda x: x.get('nome_ator', '').lower())
        
        filmes_finais = atores_do_filme_com_personagem[:final_limit]
        print(f"DEBUG REDIS: Retornando {len(filmes_finais)} atores para o filme '{id_filme}' após limite.") # DEBUG
        return filmes_finais
        
    except AttributeError as ae:
        print(f"ERRO DE ATRIBUTO NO REDIS (provavelmente decode) ao buscar atores do filme '{id_filme}': {ae}") # DEBUG
        raise DatabaseInteractionError(f"Erro de atributo ao buscar atores do filme '{id_filme}' no Redis: {ae}")
    except Exception as e:
        print(f"ERRO GERAL NO REDIS ao buscar atores do filme '{id_filme}': {e}") # DEBUG
        # import traceback
        # traceback.print_exc()
        raise DatabaseInteractionError(f"Erro ao buscar atores do filme '{id_filme}' no Redis: {e}")
    
# --- ATUALIZAÇÃO ---
def atualizar_campo_filme(r: redis.Redis, id_filme: str, campo_para_atualizar: str, novo_valor: Any) -> Optional[Dict[str, Any]]:
    chave_filme = f"{FILME_KEY_PREFIX}{str(id_filme)}"
    try:
        if not r.exists(chave_filme):
            raise ItemNotFoundError(f"Filme com ID '{id_filme}' não encontrado no Redis para atualização.")

        # Se o campo for 'generos' ou 'ano_lancamento' ou 'tipo', precisa remover dos índices antigos e adicionar aos novos
        filme_antigo_dict = _deserialize_redis_filme(r.hgetall(chave_filme)) # Pega o estado atual

        # Lógica para atualizar índices (exemplo para generos)
        if campo_para_atualizar == "generos":
            generos_antigos = set(filme_antigo_dict.get("generos", []))
            generos_novos = set(_limpar_generos_redis(novo_valor)) # Limpa o novo valor
            
            for g_antigo in generos_antigos - generos_novos: # Gêneros removidos
                r.srem(f"{IDX_FILME_GENERO_PREFIX}{_serialize_redis_value(g_antigo)}", id_filme)
            for g_novo in generos_novos - generos_antigos: # Gêneros adicionados
                r.sadd(f"{IDX_FILME_GENERO_PREFIX}{_serialize_redis_value(g_novo)}", id_filme)
            valor_serializado = _serialize_redis_value(list(generos_novos)) # Salva como lista
        elif campo_para_atualizar == "ano_lancamento":
            ano_antigo = filme_antigo_dict.get("ano_lancamento")
            if ano_antigo is not None: r.srem(f"{IDX_FILME_ANO_PREFIX}{ano_antigo}", id_filme)
            if novo_valor is not None: r.sadd(f"{IDX_FILME_ANO_PREFIX}{int(novo_valor)}", id_filme)
            valor_serializado = _serialize_redis_value(int(novo_valor) if novo_valor is not None else None)
        elif campo_para_atualizar == "tipo":
            tipo_antigo = filme_antigo_dict.get("tipo")
            if tipo_antigo: r.srem(f"{IDX_FILME_TIPO_PREFIX}{_serialize_redis_value(tipo_antigo)}", id_filme)
            if novo_valor: r.sadd(f"{IDX_FILME_TIPO_PREFIX}{_serialize_redis_value(novo_valor)}", id_filme)
            valor_serializado = _serialize_redis_value(novo_valor)
        else:
            valor_serializado = _serialize_redis_value(novo_valor)

        r.hset(chave_filme, campo_para_atualizar, valor_serializado)
        return buscar_filme_por_id(r, id_filme) # Retorna o filme atualizado
    except ItemNotFoundError: raise
    except Exception as e:
        raise DatabaseInteractionError(f"Erro ao atualizar campo '{campo_para_atualizar}' do filme '{id_filme}' no Redis: {e}")

# src/databases/redis/crud.py

def remover_filme(r: redis.Redis, id_filme: str) -> bool:
    chave_filme = f"{FILME_KEY_PREFIX}{str(id_filme)}"
    try:
        filme_hash_str = r.hgetall(chave_filme) # Já retorna Dict[str, str]
        filme_dados = _deserialize_redis_filme(filme_hash_str) # _deserialize_redis_filme espera Dict[str, str] - OK
        
        if not filme_dados:
            raise ItemNotFoundError(f"Filme com ID '{id_filme}' não encontrado no Redis para remoção.")

        deleted_count = r.delete(chave_filme)
        if deleted_count == 0:
            # Esta checagem é um pouco redundante se filme_dados foi encontrado, mas não faz mal
            raise ItemNotFoundError(f"Filme com ID '{id_filme}' não pôde ser removido (já não existia após checagem inicial?).")

        # Limpar dos índices (OK, _serialize_redis_value e os valores de filme_dados já são strings)
        if "generos" in filme_dados and filme_dados["generos"]:
            for genero in filme_dados["generos"]: 
                r.srem(f"{IDX_FILME_GENERO_PREFIX}{_serialize_redis_value(genero)}", id_filme)
        if "ano_lancamento" in filme_dados and filme_dados["ano_lancamento"] is not None:
            r.srem(f"{IDX_FILME_ANO_PREFIX}{filme_dados['ano_lancamento']}", id_filme) # ano_lancamento já é string ou int aqui, _serialize_redis_value não é estritamente necessário mas não quebra
        if "tipo" in filme_dados and filme_dados["tipo"]:
            r.srem(f"{IDX_FILME_TIPO_PREFIX}{_serialize_redis_value(filme_dados['tipo'])}", id_filme)

        # Remover das listas de elenco (ator_filmes)
        # A chave ELENCO_FILME_ATORES_PREFIX deve ser a mesma usada em inserir_elenco
        atores_deste_filme_ids_strings = r.smembers(f"{ELENCO_FILME_ATORES_PREFIX}{id_filme}") # Já retorna Set[str]
        
        print(f"DEBUG REDIS (Remover): Atores encontrados para o filme '{id_filme}': {atores_deste_filme_ids_strings}") # DEBUG

        for ator_id_str_loop in atores_deste_filme_ids_strings: # ator_id_str_loop JÁ É UMA STRING
            # A chave ELENCO_ATOR_FILMES_PREFIX deve ser a mesma usada em inserir_elenco
            r.srem(f"{ELENCO_ATOR_FILMES_PREFIX}{ator_id_str_loop}", id_filme) # SEM .decode()
            print(f"DEBUG REDIS (Remover): Removendo filme '{id_filme}' do set do ator '{ator_id_str_loop}'") # DEBUG
        
        r.delete(f"{ELENCO_FILME_ATORES_PREFIX}{id_filme}") # Deleta o set de atores para este filme
        
        print(f"INFO REDIS (Remover): Filme '{id_filme}' e suas referências de elenco removidos.") # DEBUG
        return True
    except ItemNotFoundError: 
        raise # Re-levanta para ser tratado pelo serviço/API
    except Exception as e:
        print(f"ERRO REDIS (Remover): Erro ao remover filme '{id_filme}': {e}") # DEBUG
        # import traceback # Descomente para debug mais profundo
        # traceback.print_exc()
        raise DatabaseInteractionError(f"Erro ao remover filme '{id_filme}' do Redis: {e}")

# --- AGREGAÇÕES (Muito ineficientes no Redis sem RediSearch ou modelagem específica) ---
# src/databases/redis/crud.py

def contagem_por_ano(r: redis.Redis) -> List[Dict[str, Any]]:
    print("INFO REDIS: Iniciando contagem por ano (via SCAN, pode ser ineficiente).")
    anos_contagem: Dict[int, int] = {} # Tipagem para clareza
    
    # r.scan_iter com decode_responses=True já retorna chaves como strings
    for chave_filme_str in r.scan_iter(match=f"{FILME_KEY_PREFIX}*"): 
        # r.hget com decode_responses=True já retorna o valor como string (ou None)
        ano_como_string = r.hget(chave_filme_str, "ano_lancamento") 
        
        if ano_como_string: # Verifica se não é None e não é string vazia
            try:
                ano = int(ano_como_string) # Converte a string diretamente para int
                anos_contagem[ano] = anos_contagem.get(ano, 0) + 1
            except ValueError:
                print(f"DEBUG REDIS (Contagem Ano): Valor de ano não numérico '{ano_como_string}' na chave '{chave_filme_str}'. Pulando.")
                continue
    
    resultado_formatado = [{"ano": k, "quantidade": v} for k, v in sorted(anos_contagem.items())]
    print(f"INFO REDIS (Contagem Ano): Contagem finalizada. {len(resultado_formatado)} anos distintos encontrados.") # DEBUG
    return resultado_formatado

# src/databases/redis/crud.py

def media_notas_por_genero(r: redis.Redis) -> List[Dict[str, Any]]:
    print("INFO REDIS: Iniciando média de notas por gênero (via SCAN, pode ser ineficiente).")
    generos_data: Dict[str, Dict[str, Any]] = {} 
    
    # r.scan_iter já retorna chaves como strings
    for chave_filme_str in r.scan_iter(match=f"{FILME_KEY_PREFIX}*"):
        # r.hmget com decode_responses=True retorna uma lista de strings (ou Nones)
        valores_hash_str = r.hmget(chave_filme_str, "generos", "nota") 
        
        generos_json_string = valores_hash_str[0] # Já é string (pode ser None)
        nota_como_string = valores_hash_str[1]    # Já é string (pode ser None)

        if generos_json_string and nota_como_string: # Checa se ambos os valores foram recuperados
            try:
                # generos_json_string é o JSON da lista de generos
                # nota_como_string é a string da nota
                nota_filme = float(nota_como_string) 
                
                # É crucial que _serialize_redis_value para listas use json.dumps,
                # e que os generos sejam realmente uma lista JSON válida no Redis.
                generos_filme_lista = json.loads(generos_json_string) if generos_json_string else []
                
                if not isinstance(generos_filme_lista, list): # Checagem extra
                    print(f"DEBUG REDIS (Media Gênero): 'generos' não é uma lista após json.loads para chave '{chave_filme_str}'. Valor: '{generos_json_string}'. Pulando.")
                    continue

                for genero_item in generos_filme_lista:
                    if not isinstance(genero_item, str) or not genero_item.strip(): # Ignora generos vazios ou não-string
                        continue
                    if genero_item not in generos_data:
                        generos_data[genero_item] = {"soma_nota": 0.0, "contagem": 0}
                    generos_data[genero_item]["soma_nota"] += nota_filme
                    generos_data[genero_item]["contagem"] += 1
            except json.JSONDecodeError:
                print(f"DEBUG REDIS (Media Gênero): Erro ao decodificar JSON de generos '{generos_json_string}' na chave '{chave_filme_str}'. Pulando.")
                continue
            except (ValueError, TypeError) as e_conv: # Erro ao converter nota para float, por exemplo
                print(f"DEBUG REDIS (Media Gênero): Erro de conversão (nota: '{nota_como_string}') na chave '{chave_filme_str}': {e_conv}. Pulando.")
                continue
    
    resultado_final = []
    for genero, dados in generos_data.items():
        if dados["contagem"] > 0:
            media = round(dados["soma_nota"] / dados["contagem"], 2) # Arredondando para 2 casas decimais
            resultado_final.append({"genero": genero, "media_nota": media})
    
    resultado_ordenado = sorted(resultado_final, key=lambda x: x.get("media_nota", 0.0), reverse=True)
    print(f"INFO REDIS (Media Gênero): Cálculo finalizado. {len(resultado_ordenado)} gêneros com médias calculadas.") # DEBUG
    return resultado_ordenado

# src/databases/redis/crud.py

# ... (todas as outras importações e funções auxiliares como _limpar_generos_redis, etc., permanecem iguais) ...

# A função carregar_dados_redis é a que vamos substituir inteiramente.
# As outras (inserir_filme, buscar_filme_por_id, etc.) podem continuar como estão.

# src/databases/redis/crud.py

# ... (importações e outras funções permanecem iguais) ...

# src/databases/redis/crud.py
# Adicione 'import time' no início do arquivo se ainda não tiver.
import time

# ... (outras importações e funções auxiliares permanecem as mesmas) ...

def carregar_dados_redis(
    r: redis.Redis,
    filmes_path: str,
    atores_path: str,
    elenco_path: str
) -> Dict[str, Any]:
    # Limpeza seletiva (sem alterações)
    print("Limpando dados antigos do Redis para nova carga...")
    for key_pattern in [
        f"{FILME_KEY_PREFIX}*", f"{ATOR_KEY_PREFIX}*", f"{ELENCO_FILME_ATORES_PREFIX}*",
        f"{ELENCO_ATOR_FILMES_PREFIX}*", f"{PERSONAGEM_PROPS_KEY_PREFIX}*",
        "idx:filme:genero:*", "idx:filme:ano:*", "idx:filme:tipo:*"
    ]:
        keys_to_delete = list(r.scan_iter(match=key_pattern))
        if keys_to_delete:
            r.delete(*keys_to_delete)
    
    erros_carga = []
    counts = {"filmes": 0, "atores": 0, "elenco": 0}

    pipe = r.pipeline()
    print("Pipeline único inicializado. Processando todos os arquivos...")

    # Processamento dos 3 arquivos (filmes, atores, elenco) - sem alterações
    # ... (cole aqui toda a sua lógica de leitura dos 3 dataframes e adição de comandos ao 'pipe')
    # --- Carregar Filmes ---
    try:
        filmes_df = pd.read_csv(filmes_path, sep="\t", keep_default_na=False, na_values=['\\N', ''])
        for idx, row in filmes_df.iterrows():
            try:
                filme_id = str(row['titulo_id'])
                chave_filme = f"{FILME_KEY_PREFIX}{filme_id}"
                filme_dados = {'_id': filme_id, 'titulo': str(row['titulo']) if pd.notna(row['titulo']) else None,'tipo': str(row['tipo']) if pd.notna(row['tipo']) else None,'ano_lancamento': int(row['ano_lancamento']) if pd.notna(row['ano_lancamento']) and row['ano_lancamento'] != '' else None,'generos': _limpar_generos_redis(row.get('generos')),'nota': float(row['nota']) if pd.notna(row['nota']) and row['nota'] != '' else None,'numero_votos': int(row['numero_votos']) if pd.notna(row['numero_votos']) and row['numero_votos'] != '' else None,'duracao': int(row['duracao']) if pd.notna(row['duracao']) and row['duracao'] != '' else None,'sinopse': str(row['sinopse']) if pd.notna(row['sinopse']) else None}
                payload_redis = {k: _serialize_redis_value(v) for k, v in filme_dados.items() if v is not None}
                pipe.hmset(chave_filme, payload_redis)
                if filme_dados["generos"]:
                    for genero in filme_dados["generos"]: pipe.sadd(f"{IDX_FILME_GENERO_PREFIX}{_serialize_redis_value(genero)}", filme_id)
                if filme_dados["ano_lancamento"] is not None: pipe.sadd(f"{IDX_FILME_ANO_PREFIX}{filme_dados['ano_lancamento']}", filme_id)
                if filme_dados["tipo"]: pipe.sadd(f"{IDX_FILME_TIPO_PREFIX}{_serialize_redis_value(filme_dados['tipo'])}", filme_id)
                counts["filmes"] += 1
            except Exception as e_val: erros_carga.append({"arq":"filmes_redis", "ln":idx+2, "id":row.get('titulo_id'), "err":"Val/Prep", "det":str(e_val)})
    except Exception as e_read: erros_carga.append({"arq": "filmes_redis", "tipo": "LeituraDF", "det": str(e_read)})
    # --- Carregar Atores ---
    try:
        atores_df = pd.read_csv(atores_path, sep="\t", keep_default_na=False, na_values=['\\N', ''])
        for idx, row in atores_df.iterrows():
            try:
                ator_id = str(row['ator_id'])
                chave_ator = f"{ATOR_KEY_PREFIX}{ator_id}"
                ator_dados = {'_id': ator_id, 'nome_ator': str(row['nome_ator']) if pd.notna(row['nome_ator']) else None, 'ano_nascimento': int(row['ano_nascimento']) if pd.notna(row['ano_nascimento']) and row['ano_nascimento'] != '' else None}
                payload_redis = {k: _serialize_redis_value(v) for k, v in ator_dados.items() if v is not None}
                pipe.hmset(chave_ator, payload_redis)
                counts["atores"] += 1
            except Exception as e_val: erros_carga.append({"arq":"atores_redis", "ln":idx+2, "id":row.get('ator_id'), "err":"Val/Prep", "det":str(e_val)})
    except Exception as e_read: erros_carga.append({"arq": "atores_redis", "tipo": "LeituraDF", "det": str(e_read)})
    # --- Carregar Elenco ---
    try:
        elenco_df = pd.read_csv(elenco_path, sep="\t", keep_default_na=False, na_values=['\\N', ''])
        for idx, row in elenco_df.iterrows():
            try:
                ator_id = str(row['ator_id'])
                titulo_id = str(row['titulo_id'])
                nome_personagem = str(row['nome_personagem']) if pd.notna(row['nome_personagem']) else None
                pipe.sadd(f"{ELENCO_FILME_ATORES_PREFIX}{titulo_id}", ator_id)
                pipe.sadd(f"{ELENCO_ATOR_FILMES_PREFIX}{ator_id}", titulo_id)
                if nome_personagem:
                    chave_props = f"{PERSONAGEM_PROPS_KEY_PREFIX}{titulo_id}:{ator_id}"
                    pipe.hset(chave_props, "nome", _serialize_redis_value(nome_personagem))
                counts["elenco"] += 1
            except Exception as e_val: erros_carga.append({"arq":"elenco_redis", "ln":idx+2, "ids":f"A:{row.get('ator_id')},F:{row.get('titulo_id')}", "err":"Val/Prep", "det":str(e_val)})
    except Exception as e_read: erros_carga.append({"arq": "elenco_redis", "tipo": "LeituraDF", "det": str(e_read)})


    # MUDANÇA AQUI: Medindo apenas a execução do pipeline
    print("Enviando todos os comandos para o Redis de uma só vez...")
    start_redis_exec = time.perf_counter()
    pipe.execute()
    end_redis_exec = time.perf_counter()
    redis_exec_time = end_redis_exec - start_redis_exec
    print(f"Todos os dados foram carregados no Redis. Tempo de execução PURO do pipeline: {redis_exec_time:.4f}s")

    # --- Mensagem Final e Retorno ---
    msg = f"Carga Redis Otimizada (Pipeline Único): {counts['filmes']} filmes, {counts['atores']} atores, {counts['elenco']} relações."
    status_op = "sucesso" if not erros_carga else "concluído_com_erros"
    if erros_carga: print(f"LOG DE CARGA REDIS - Erros: {erros_carga[:5]}")
    
    # MUDANÇA AQUI: Adicionando o tempo de execução puro aos detalhes
    detalhes_finais = {**counts, "tempo_execucao_redis_ms": redis_exec_time * 1000}
    
    return { "status": status_op, "message": msg, "detalhes_carga": detalhes_finais, "erros_execucao": erros_carga }