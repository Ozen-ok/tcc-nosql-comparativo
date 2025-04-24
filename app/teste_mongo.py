from pymongo import MongoClient
import pandas as pd
# Carrega os dados
df = pd.read_csv("/mnt/c/Users/ozen/Desktop/projeto_imdb/dados_filtrados.tsv", sep="\t")

# Agrupa por título
agrupado = df.groupby(
    ["titulo_id", "titulo", "tipo", "ano_lancamento", "generos", "nota", "numero_votos", "duracao", "sinopse"]
)

# Cria documentos com lista de atores
documentos = []

for chave, grupo in agrupado:
    titulo_dict = {
        "titulo_id": chave[0],
        "titulo": chave[1],
        "tipo": chave[2],
        "ano_lancamento": int(chave[3]),
        "generos": eval(chave[4]) if isinstance(chave[4], str) else chave[4],
        "nota": float(chave[5]),
        "numero_votos": int(chave[6]),
        "duracao": int(chave[7]),
        "sinopse": chave[8],
        "atores": []
    }
    
    for _, linha in grupo.iterrows():
        ator = {
            "ator_id": linha["ator_id"],
            "nome_ator": linha["nome_ator"],
            "ano_nascimento": int(linha["ano_nascimento"]),
            "nome_personagem": linha["nome_personagem"]
        }
        titulo_dict["atores"].append(ator)

    documentos.append(titulo_dict)


# Conectar ao MongoDB (substitua 'localhost' se necessário)
client = MongoClient("mongodb://localhost:27017/")

# Selecionar o banco de dados (se não existir, ele será criado)
db = client["imdb"]

# Selecionar a coleção (se não existir, ela será criada)
collection = db["titulos"]

# Inserir os documentos na coleção
collection.insert_many(documentos)

# Verificar se os dados foram inseridos corretamente
for titulo in collection.find():
    print(titulo)
