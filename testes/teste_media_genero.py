# testes/teste_media_genero.py

import requests
import time
import statistics
import json

# --- Configurações do Teste ---
API_URL = "http://localhost:8000/api/v1/analytics/filmes/media-notas-por-genero"
NUM_REPETICOES = 10
BANCOS_PARA_TESTAR = ["mongo", "cassandra", "neo4j", "redis"]

# Dicionário para armazenar os resultados
resultados_por_banco = {banco: [] for banco in BANCOS_PARA_TESTAR}

# --- Execução dos Testes ---
print(f"Iniciando teste de agregação (Média por Gênero) ({NUM_REPETICOES} rodadas)...")

for i in range(NUM_REPETICOES):
    print(f"\n--- Rodada {i + 1}/{NUM_REPETICOES} ---")
    for banco in BANCOS_PARA_TESTAR:
        print(f"Testando média para o banco: {banco}...")
        
        params = {"banco": banco}
        
        inicio = time.perf_counter()
        try:
            response = requests.get(API_URL, params=params, timeout=60)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"  ERRO ao testar {banco}: {e}")
            continue

        fim = time.perf_counter()
        
        duracao = fim - inicio
        resultados_por_banco[banco].append(duracao)
        print(f"  Tempo de cálculo da média para {banco}: {duracao:.4f} segundos")
        
        time.sleep(1) # Pequena pausa

print("\n--- Teste de Média por Gênero Concluído ---")

# --- Análise e Salvamento ---
print("\n## Análise dos Resultados de Média por Gênero ##")
for banco, tempos in resultados_por_banco.items():
    if tempos:
        media = statistics.mean(tempos)
        desvio_padrao = statistics.stdev(tempos) if len(tempos) > 1 else 0
        print(f"\nBanco: {banco.upper()}")
        print(f"  - Média: {media:.4f} segundos")
        print(f"  - Desvio Padrão: {desvio_padrao:.4f} segundos")

with open("testes/resultados_media_genero.json", "w") as f:
    json.dump(resultados_por_banco, f, indent=4)

print("\nResultados salvos em 'testes/resultados_media_genero.json'")