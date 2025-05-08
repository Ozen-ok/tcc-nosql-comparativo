from neo4j import GraphDatabase
from config.db_config import NEO4J_HOST, NEO4J_PORT, NEO4J_USER, NEO4J_PASSWORD

def get_neo4j_connection():
    uri = f"bolt://{NEO4J_HOST}:{NEO4J_PORT}"
    driver = GraphDatabase.driver(uri, auth=(NEO4J_USER, NEO4J_PASSWORD))
    return driver

def close_neo4j_connection(driver):
    driver.close()  # Fecha o driver

# Uso da sessão com context manager
def execute_query(driver, query):
    with driver.session() as session:  # Usando context manager para a sessão
        result = session.run(query)
        return result

def execute_query_with_driver(query):
    with get_neo4j_connection() as driver:  # Usando o context manager para o driver
        return execute_query(driver, query)
