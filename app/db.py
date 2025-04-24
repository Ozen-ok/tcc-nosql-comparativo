from pymongo import MongoClient
import redis
from cassandra.cluster import Cluster
from neo4j import GraphDatabase

# Conexão com MongoDB
def get_mongo_data():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["meubanco"]
    collection = db["minhacolecao"]
    return collection.find_one({"key": "value"})

# Conexão com Redis
def get_redis_data():
    r = redis.Redis(host="localhost", port=6379, db=0)
    return r.get("mykey")

# Conexão com Cassandra
def get_cassandra_data():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('meubanco')
    rows = session.execute("SELECT * FROM minha_tabela")
    return [row for row in rows]

# Conexão com Neo4j
def get_neo4j_data():
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "senha"))
    session = driver.session()
    result = session.run("MATCH (n) RETURN n LIMIT 5")
    return [record["n"] for record in result]

def test_mongo_connection():
    try:
        client = MongoClient("mongodb://localhost:27017")  # O endereço padrão do MongoDB
        db = client.test  # Tente acessar um banco de dados de teste
        db.command("ping")  # Comando simples para verificar se a conexão está ativa
        return {"status": "MongoDB connected successfully!"}
    except Exception as e:
        return {"status": f"MongoDB connection failed: {str(e)}"}
    
def test_redis_connection():
    try:
        client = redis.Redis(host='localhost', port=6379, decode_responses=True)
        client.set('teste', 'sucesso')  # Inserindo um valor simples
        return {"status": "Redis connected successfully!", "valor": client.get('teste')}
    except Exception as e:
        return {"status": f"Redis connection failed: {str(e)}"} 

def test_neo4j_connection():
    try:
        uri = "bolt://localhost:7687"  # Endereço padrão do Neo4j
        driver = GraphDatabase.driver(uri, auth=("neo4j", "minhasenha123"))  # Altere a senha se necessário
        session = driver.session()
        session.run("RETURN 1")  # Comando simples para verificar a conexão
        session.close()
        return {"status": "Neo4j connected successfully!"}
    except Exception as e:
        return {"status": f"Neo4j connection failed: {str(e)}"}
    
def test_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])  # Endereço do Cassandra
        session = cluster.connect()
        session.execute("USE novo_keyspace")  # Substitua pelo seu keyspace
        return {"status": "Cassandra connected successfully!"}
    except Exception as e:
        return {"status": f"Cassandra connection failed: {str(e)}"}