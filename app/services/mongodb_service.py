# services/mongodb_service.py
from pymongo import MongoClient
from config.db_config import MONGO_HOST, MONGO_PORT, MONGO_DB_NAME

def get_mongo_connection():
    client = MongoClient(MONGO_HOST, int(MONGO_PORT))
    db = client[MONGO_DB_NAME]
    return db, client

def close_mongo_connection(client):
    client.close()  