# testes/teste_insercao.py
# ... (importações e configurações iniciais sem alterações) ...
import requests, time, statistics, json

API_URL = "http://localhost:8000/api/v1/admin/carregar-base"
NUM_REPETICOES = 10
BANCOS_PARA_TESTAR = ["mongo", "cassandra", "neo4j", "redis"]
PAYLOAD_CARGA = {"filmes_path": "data/filmes.tsv", "atores_path": "data/atores.tsv", "elenco_path": "data/elenco.tsv"}

# MUDANÇA AQUI: Dicionário para armazenar o tempo puro do Redis
resultados_por_banco = {banco: [] for banco in BANCOS_PARA_TESTAR}
tempos_puros_redis = []

print(f"Iniciando teste de inserção em lote ({NUM_REPETICOES} rodadas)...")
for i in range(NUM_REPETICOES):
    print(f"\n--- Rodada {i + 1}/{NUM_REPETICOES} ---")
    for banco in BANCOS_PARA_TESTAR:
        print(f"Testando inserção para o banco: {banco}...")
        params = {"banco": banco}
        inicio = time.perf_counter()
        try:
            response = requests.post(API_URL, params=params, json=PAYLOAD_CARGA, timeout=300)
            response.raise_for_status()
            response_json = response.json() # Pega a resposta JSON
        except requests.exceptions.RequestException as e:
            print(f"  ERRO ao testar {banco}: {e}")
            continue
        fim = time.perf_counter()
        duracao = fim - inicio
        resultados_por_banco[banco].append(duracao)
        print(f"  Tempo TOTAL de inserção para {banco}: {duracao:.4f} segundos")
        
        # MUDANÇA AQUI: Captura e exibe o tempo puro do Redis
        if banco == 'redis':
            tempo_ms = response_json.get("detalhes_carga", {}).get("tempo_execucao_redis_ms")
            if tempo_ms is not None:
                tempos_puros_redis.append(tempo_ms / 1000) # Converte para segundos para a média
                print(f"  -> Tempo PURO de execução no Redis: {tempo_ms:.2f} ms")
        
        time.sleep(2)

print("\n--- Teste de Inserção Concluído ---")
# --- Análise dos Resultados (com a nova métrica) ---
print("\n## Análise dos Resultados de Inserção ##")
for banco, tempos in resultados_por_banco.items():
    if tempos:
        media = statistics.mean(tempos)
        desvio_padrao = statistics.stdev(tempos) if len(tempos) > 1 else 0
        print(f"\nBanco: {banco.upper()} (Tempo Total)")
        print(f"  - Média: {media:.4f} segundos")
        print(f"  - Desvio Padrão: {desvio_padrao:.4f} segundos")

if tempos_puros_redis:
    media_redis_puro = statistics.mean(tempos_puros_redis)
    desvio_redis_puro = statistics.stdev(tempos_puros_redis) if len(tempos_puros_redis) > 1 else 0
    print(f"\nBanco: REDIS (Tempo de Execução Puro no Servidor)")
    print(f"  - Média: {media_redis_puro:.4f} segundos ({media_redis_puro*1000:.2f} ms)")
    print(f"  - Desvio Padrão: {desvio_redis_puro:.4f} segundos")

# Salva os resultados totais como antes
with open("testes/resultados_insercao.json", "w") as f:
    json.dump(resultados_por_banco, f, indent=4)
print("\nResultados totais salvos em 'testes/resultados_insercao.json'")