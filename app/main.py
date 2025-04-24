from fastapi import FastAPI, HTTPException
from db import get_mongo_data, get_redis_data, get_cassandra_data, get_neo4j_data, test_mongo_connection, test_cassandra_connection, test_neo4j_connection, test_redis_connection

from pymongo import MongoClient
from bson.objectid import ObjectId
import redis
from neo4j import GraphDatabase
from cassandra.cluster import Cluster

app = FastAPI()

# Conexão com MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["meu_keyspace"]  # Nome do seu banco de dados
collection = db["meu_colecao"]  # Nome da coleção (tabela) que você deseja acessar

@app.post("/add_mongo/")
def add_mongo(data: dict):
    collection = db["meu_colecao"]
    result = collection.insert_one(data)
    return {"message": "Data inserted", "id": str(result.inserted_id)}

@app.get("/get_mongo/{id}")
async def get_mongo(id: str):
    # Verifica se o ObjectId é válido
    if not ObjectId.is_valid(id):
        raise HTTPException(status_code=400, detail="Invalid ObjectId format")
    
    result = collection.find_one({"_id": ObjectId(id)})
    
    if result:
        # Converte o _id do ObjectId para string
        result["_id"] = str(result["_id"])
        return {"data": result}
    else:
        raise HTTPException(status_code=404, detail="Data not found")

@app.post("/add_cassandra/")
def add_cassandra(data: dict):
    query = "INSERT INTO minha_tabela (id, nome) VALUES (%s, %s)"
    session.execute(query, (data["id"], data["nome"]))
    return {"message": "Data inserted"}

@app.get("/get_cassandra/{id}")
def get_cassandra(id: str):
    query = "SELECT * FROM minha_tabela WHERE id=%s"
    rows = session.execute(query, (id,))
    for row in rows:
        return {"data": row}
    return {"error": "Data not found"}

# Conexão com Neo4j
uri = "bolt://localhost:7687"  # Altere para o IP do Neo4j, se necessário
driver = GraphDatabase.driver(uri, auth=("neo4j", "senha"))  # Substitua pela sua senha

@app.post("/add_neo4j/")
def add_neo4j(data: dict):
    with driver.session() as session:
        session.run("CREATE (n:Person {name: $name})", name=data["name"])
    return {"message": "Node added"}

@app.get("/get_neo4j/{name}")
def get_neo4j(name: str):
    with driver.session() as session:
        result = session.run("MATCH (n:Person {name: $name}) RETURN n", name=name)
        for record in result:
            return {"data": record["n"]}
    return {"error": "Node not found"}


# Conexão com Redis
r = redis.Redis(host='localhost', port=6379, db=0)

@app.post("/add_redis/")
def add_redis(data: dict):
    r.set(data["key"], data["value"])
    return {"message": "Data inserted"}

@app.get("/get_redis/{key}")
def get_redis(key: str):
    value = r.get(key)
    if value:
        return {"data": value.decode("utf-8")}
    return {"error": "Data not found"}

@app.get("/")
def read_root():
    return {"message": "Hello World"}

@app.get("/mongo")
def get_mongo_data_route():
    return {"data": get_mongo_data()}

@app.get("/redis")
def get_redis_data_route():
    return {"data": get_redis_data()}

@app.get("/cassandra")
def get_cassandra_data_route():
    return {"data": get_cassandra_data()}

@app.get("/neo4j")
def get_neo4j_data_route():
    return {"data": get_neo4j_data()}

@app.get("/test-mongo")
def test_mongo():
    return test_mongo_connection()

@app.get("/test-redis")
def test_redis():
    return test_redis_connection()

@app.get("/test-neo4j")
def test_neo4j():
    return test_neo4j_connection()

@app.get("/test-cassandra")
def test_cassandra():
    return test_cassandra_connection()