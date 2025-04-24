import pandas as pd
from translatepy import Translator
import time

# Arquivo de entrada e saída
input_csv = "sinopses_jogos_igdb.csv"
output_csv = "sinopses_jogos_igdb_traduzido.csv"

# Carrega o CSV
df = pd.read_csv(input_csv)
df = df.dropna(subset=["summary"])  # Remove linhas sem sinopse

# Inicializa o tradutor
translator = Translator()

# Lista para armazenar sinopses traduzidas
translated = []

for i, summary in enumerate(df["summary"]):
    try:
        translated_text = translator.translate(summary, "Portuguese").result
        translated.append(translated_text)
        print(f"[{i+1}] ✅ Traduzido com sucesso")
    except Exception as e:
        translated.append("")  # Se falhar, coloca string vazia
        print(f"[{i+1}] ⚠️ Erro na tradução: {e}")
    
    time.sleep(1.5)  # Pausa pra evitar bloqueios

# Adiciona a nova coluna ao DataFrame
df["summary_ptbr"] = translated

# Salva o resultado
df.to_csv(output_csv, index=False)
print(f"\n✅ Traduções salvas em: {output_csv}")
