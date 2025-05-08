from cassandra.cluster import Cluster
from config.db_config import CASSANDRA_HOST, CASSANDRA_PORT
from time import sleep

def get_cassandra_connection():
    cluster = Cluster([CASSANDRA_HOST], port=int(CASSANDRA_PORT))
    session = None
    retry_count = 5  # Número de tentativas de conexão
    while retry_count > 0:
        try:
            session = cluster.connect()
            break  # Conexão bem-sucedida, sai do loop
        except Exception as e:
            print(f"Erro de conexão com Cassandra: {e}. Tentando novamente...")
            retry_count -= 1
            sleep(5)  # Espera 5 segundos antes de tentar novamente
    if session is None:
        raise Exception("Falha ao conectar ao Cassandra após várias tentativas.")
    return session, cluster  # Retorna tanto a sessão quanto o cluster

def close_cassandra_connection(cluster):
    cluster.shutdown()  # Fecha a conexão com o Cassandra
