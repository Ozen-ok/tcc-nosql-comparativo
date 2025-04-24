import requests
import pandas as pd
from time import sleep

# ================
# 🔐 AUTENTICAÇÃO
# ================
client_id = "33q3ujbc377dz4l19w7vbgpy4f9hwe"
client_secret = "je0u76eczx5rbtbnlotp9oqjm2vqvn"

def gerar_token(client_id, client_secret):
    url = "https://id.twitch.tv/oauth2/token"
    params = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }
    res = requests.post(url, params=params)
    return res.json()["access_token"]

access_token = gerar_token(client_id, client_secret)

headers = {
    "Client-ID": client_id,
    "Authorization": f"Bearer {access_token}"
}

# ======================
# 📄 ENTRADA DOS DADOS
# ======================
df = pd.read_csv("dados_filtrados.tsv", sep="\t", dtype=str)
df_jogos = df[df["type"] == "videoGame"]
nomes = df_jogos["title"].dropna().unique()

falhas = []
resultados = []

# ========================
# 🎮 BUSCA DADOS DOS JOGOS
# ========================
def buscar_info_jogo(nome):
    query = f'''
        search "{nome}";
        fields name, summary;
        limit 1;
    '''
    r = requests.post("https://api.igdb.com/v4/games", headers=headers, data=query)

    if r.status_code != 200:
        falhas.append({"title": nome, "erro": f"HTTP {r.status_code}"})
        print(f"Erro ao buscar {nome}: {r.status_code}")
        return

    data = r.json()
    if not data:
        falhas.append({"title": nome, "erro": "Jogo não encontrado"})
        print(f"Nenhum resultado para: {nome}")
        return

    jogo = data[0]

    titulo = jogo.get("name", "")
    sinopse = jogo.get("summary", "")

    resultados.append({
        "title": titulo,
        "summary": sinopse
    })

    print(f"✅ Dados salvos: {nome}")

# ============
# 🔁 LOOP
# ============
for nome in nomes:
    buscar_info_jogo(nome)
    sleep(0.25)

# ========================
# 💾 SALVA RESULTADOS
# ========================
if resultados:
    df_resultados = pd.DataFrame(resultados)
    df_resultados.to_csv("sinopses_jogos_igdb.csv", index=False)
    print(f"\n💾 {len(resultados)} jogos salvos em sinopses_jogos_igdb.csv")

if falhas:
    df_falhas = pd.DataFrame(falhas)
    df_falhas.to_csv("falhas_igdb.csv", index=False)
    print(f"🚫 {len(falhas)} falhas registradas em falhas_igdb.csv")
else:
    print("\n🎉 Nenhuma falha!")
