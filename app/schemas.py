import pandas as pd
import requests
import time

# ==========================
# 🔧 CONFIGURAÇÕES INICIAIS
# ==========================
api_key = "454a12a26f3c8ed19f53eb9bdc85ba3d"
input_tsv = "dados_filtrados.tsv"
output_csv = "sinopses_tmdb.csv"

# ==========================
# 📄 LEITURA E FILTRO
# ==========================
df = pd.read_csv(input_tsv, sep="\t", usecols=["title_id", "title", "type"])
df = df[df["type"] != "videoGame"]
df = df.dropna(subset=["title_id"])
df = df.drop_duplicates(subset="title_id")
ids = df["title_id"].tolist()

print(f"\n🎬 Total de títulos (exceto jogos): {len(ids)}\n")

# ==========================
# 🔎 BUSCA SINOPSES
# ==========================
resultados = []
falhas = []

for i, imdb_id in enumerate(ids):
    if i > 0 and i % 40 == 0:
        print("⏳ Pausa de 10s para evitar rate limit...")
        time.sleep(10)

    url = f"https://api.themoviedb.org/3/find/{imdb_id}"
    params = {
        "api_key": api_key,
        "external_source": "imdb_id"
    }

    try:
        r = requests.get(url, params=params)
        data = r.json()

        results = data.get("movie_results") or data.get("tv_results")
        if results and "overview" in results[0]:
            overview = results[0]["overview"]
            resultados.append({
                "imdb_id": imdb_id,
                "title": df.loc[df["title_id"] == imdb_id, "title"].values[0],
                "overview": overview,
                "type": df.loc[df["title_id"] == imdb_id, "type"].values[0]
            })
            print(f"[{i+1}] ✅ {imdb_id} - Sinopse coletada")
        else:
            falhas.append(imdb_id)
            print(f"[{i+1}] ❌ {imdb_id} - Sem sinopse encontrada")

    except Exception as e:
        falhas.append(imdb_id)
        print(f"[{i+1}] ⚠️ Erro ao buscar {imdb_id}: {e}")

# ==========================
# 💾 SALVA RESULTADOS
# ==========================
pd.DataFrame(resultados).to_csv(output_csv, index=False)

if falhas:
    with open("falhas_tmdb.txt", "w") as f:
        for imdb_id in falhas:
            f.write(imdb_id + "\n")
    print(f"\n🚫 {len(falhas)} falhas registradas em falhas_tmdb.txt")

print(f"\n✅ Sinopses salvas em: {output_csv}")
print(f"Total de sinopses encontradas: {len(resultados)}")
