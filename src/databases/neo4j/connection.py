# src/databases/neo4j/connection.py
# (Seu código atual está bom)
from neo4j import GraphDatabase, Driver # Adicionei Driver para tipagem
from src.core.db_config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD    
from typing import Optional

# Importe suas exceções customizadas se quiser tratar ConnectionError de forma mais específica aqui
# from src.core.exceptions import DatabaseInteractionError

driver_neo4j: Optional[Driver] = None

def get_neo4j_driver() -> Driver:
    """Retorna o driver Neo4j para conexões, inicializando se necessário."""
    global driver_neo4j
    if driver_neo4j is None:
        try:
            driver_neo4j = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
            # Verifica a conectividade
            driver_neo4j.verify_connectivity()
            print("Conexão com Neo4j estabelecida e verificada.")
        except Exception as e:
            # Em vez de ConnectionError genérico, poderia ser uma exceção customizada
            # raise DatabaseInteractionError(f"Falha ao conectar ou verificar o Neo4j: {e}") from e
            raise ConnectionError(f"Falha ao conectar no Neo4j: {e}") from e
    return driver_neo4j

def close_neo4j_driver():
    """Fecha o driver do Neo4j se estiver aberto."""
    global driver_neo4j
    if driver_neo4j is not None:
        driver_neo4j.close()
        driver_neo4j = None
        print("Conexão com Neo4j fechada.")

# Nota: Em uma aplicação FastAPI, o ciclo de vida do driver (abrir ao iniciar, fechar ao desligar)
# pode ser gerenciado com eventos de startup/shutdown do FastAPI.
# Por agora, get_neo4j_driver() cria se não existir.