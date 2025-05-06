# tests/test_data_loader.py
from pymongo import MongoClient

try:
    uri = "mongodb://root:ImdbSecure2025@localhost:27017/admin"
    client = MongoClient(uri)
    client.admin.command("ping")
    print("Conectado com sucesso ao MongoDB!")
except Exception as e:
    print("Erro na conexão:", e)
