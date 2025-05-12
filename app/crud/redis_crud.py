import uuid
import json
from config.db_config import get_redis_client 
from typing import List
r = get_redis_client()

# INSERÇÕES -----------------------------------------------

def inserir_filme(r, filme: dict):
    titulo_id = filme["titulo_id"]
    filme_preparado = filme.copy()
    if isinstance(filme_preparado.get("generos"), list):
        filme_preparado["generos"] = json.dumps(filme_preparado["generos"])
    filme_preparado = {k: str(v) for k, v in filme_preparado.items()}
    r.hset(f"filme:{titulo_id}", mapping=filme_preparado)

def inserir_ator(r, ator: dict):
    ator_id = ator["ator_id"]
    ator = {k: str(v) for k, v in ator.items()}
    r.hset(f"ator:{ator_id}", mapping=ator)

def inserir_elenco(r, relacao: dict):
    key = f"elenco:{relacao['titulo_id']}:{relacao['ator_id']}"
    relacao = {k: str(v) for k, v in relacao.items()}
    r.hset(key, mapping=relacao)

# CONSULTAS -----------------------------------------------

def buscar_filmes_por_genero(r, generos: list):
    resultados = []
    generos_input = [g.strip().lower() for g in generos]

    for key in r.scan_iter("filme:*"):  # Iterando sobre todas as chaves de filmes
        filme = r.hgetall(key)
        
        try:
            # Decodificando e tratando a resposta do Redis
            f = {
                k if isinstance(k, str) else k.decode():
                v if isinstance(v, str) else v.decode()
                for k, v in filme.items()
            }

            if "generos" not in f:
                print(f"[⚠️] Filme {key} não possui o campo 'generos'")
                continue

            generos_filme = [g.strip().lower() for g in json.loads(f["generos"])]

            # Verificando se todos os gêneros fornecidos estão na lista de gêneros do filme
            if all(g in generos_filme for g in generos_input):
                # Adicionando o filme ao resultado
                resultados.append(f)

        except (json.JSONDecodeError, Exception) as e:
            print(f"[⚠️] Erro ao processar {key}: {e}")
            continue

    return resultados

def buscar_filmes_avancado(r, generos: List[str], ano_min: int, nota_min: float):
    resultados = []

    generos_input = [g.strip().lower() for g in generos]

    for key in r.scan_iter("filme:*"):
        filme = r.hgetall(key)
        try:
            f = {
                k if isinstance(k, str) else k.decode():
                v if isinstance(v, str) else v.decode()
                for k, v in filme.items()
            }

            if not all(c in f for c in ["generos", "ano_lancamento", "nota"]):
                print(f"[⚠️] Campos ausentes em {key}")
                continue

            generos_filme = [g.strip().lower() for g in json.loads(f["generos"])]
            ano = int(f["ano_lancamento"])
            nota = float(f["nota"])

            if (all(g in generos_filme for g in generos_input)
                and ano >= ano_min
                and nota >= nota_min):
                resultados.append(f)

        except (ValueError, json.JSONDecodeError) as e:
            print(f"[⚠️] Erro ao processar {key}: {e}")
            continue

    return resultados


# ATUALIZAÇÃO ---------------------------------------------

def atualizar_nota_filme(r, titulo_id: str, nova_nota: float):
    r.hset(f"filme:{titulo_id}", "nota", str(nova_nota))

# REMOÇÃO -------------------------------------------------

def remover_filme(r, titulo_id: str):
    r.delete(f"filme:{titulo_id}")

# AGREGAÇÃO / ANÁLISE -------------------------------------

def contar_filmes_por_ano(r):
    contagem = {}

    for key in r.scan_iter("filme:*"):
        try:
            filme = r.hgetall(key)
            ano = filme.get("ano_lancamento")  # Não usa o prefixo 'b', porque é uma string normal

            print(f"[🔍] Processando {key} - Conteúdo: {filme}")

            # Verifica se o campo 'ano_lancamento' existe
            if ano is not None:
                try:
                    # Tenta converter para inteiro
                    ano = int(ano)
                    contagem[ano] = contagem.get(ano, 0) + 1
                except ValueError:
                    print(f"[⚠️] Valor inválido para ano: {ano} em {key}")
                    continue
            else:
                print(f"[❌] Filme {key} não possui o campo 'ano_lancamento'.")

        except Exception as e:
            print(f"[⚠️] Erro ao processar o filme {key}: {e}")

    # Gerar e retornar o resultado
    try:
        resultado = [{"_id": ano, "quantidade": qtd} for ano, qtd in sorted(contagem.items(), key=lambda x: x[0])]
        return resultado
    except Exception as e:
        print(f"[❌] Erro ao gerar o resultado final: {e}")
        return []


def media_notas_por_genero(r):
    generos_dict = {}

    for key in r.scan_iter("filme:*"):
        filme = r.hgetall(key)

        # Decodifica apenas se necessário
        def seguro_decode(obj):
            return obj.decode() if isinstance(obj, bytes) else obj

        filme_decodificado = {seguro_decode(k): seguro_decode(v) for k, v in filme.items()}
        key_decodificado = seguro_decode(key)

        try:
            if "nota" not in filme_decodificado:
                print(f"[❌] Filme {key_decodificado} não possui o campo 'nota'. Chaves: {list(filme_decodificado.keys())}")
                continue

            if "generos" not in filme_decodificado:
                print(f"[❌] Filme {key_decodificado} não possui o campo 'generos'. Chaves: {list(filme_decodificado.keys())}")
                continue

            nota = float(filme_decodificado["nota"])
            generos = json.loads(filme_decodificado["generos"])

            print(f"[🔍] Analisando {key_decodificado} | nota: {nota} | generos: {generos}")

            for g in generos:
                if g not in generos_dict:
                    generos_dict[g] = {"soma": 0, "contagem": 0}
                generos_dict[g]["soma"] += nota
                generos_dict[g]["contagem"] += 1

        except Exception as e:
            print(f"[⚠️] Erro ao processar {key_decodificado}: {e}")
            continue

    medias = [
        {"genero": g, "media_nota": round(v["soma"] / v["contagem"], 2)}
        for g, v in generos_dict.items()
    ]
    return sorted(medias, key=lambda x: x["media_nota"], reverse=True)



def inserir_filme_redis(filme_dict):
   
    titulo_id = filme_dict["titulo_id"]

    try:
        # Serializa a lista de gêneros para string JSON
        if isinstance(filme_dict.get("generos"), list):
            filme_dict["generos"] = json.dumps(filme_dict["generos"])

        # Converte todos os valores do dicionário para string (por segurança)
        filme_str_dict = {k: str(v) for k, v in filme_dict.items()}

        # Insere no Redis como um hash
        r.hset(f"filme:{titulo_id}", mapping=filme_str_dict)

    except Exception as e:
        print(f"Erro ao inserir o filme no Redis: {e}")


# Inserir Ator
def inserir_ator_redis(ator):
    
    try:
        ator_id = ator.get("ator_id", str(uuid.uuid4()))
        r.hset(f"ator:{ator_id}", mapping=ator)
    except Exception as e:
        print(f"Erro ao inserir o ator: {e}")

# Inserir Elenco (Relação Ator - Filme)
def inserir_elenco_redis(elenco):
    
    try:
        rel_id = f"{elenco['ator_id']}:{elenco['titulo_id']}"
        r.hset(f"elenco:{rel_id}", mapping=elenco)
    except Exception as e:
        print(f"Erro ao inserir o elenco: {e}")
