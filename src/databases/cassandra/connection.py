# src/databases/cassandra/connection.py
from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from src.core.db_config import CASSANDRA_USER, CASSANDRA_PASSWORD, CASSANDRA_HOST, CASSANDRA_PORT, CASSANDRA_KEYSPACE
# Importe as exceções customizadas se for usá-las aqui
# from src.core.exceptions import DatabaseOperationError, DatabaseInteractionError


def _criar_schema_cassandra(session: Session, keyspace: str):
    """
    Cria o keyspace e as tabelas necessárias no Cassandra IF NOT EXISTS.
    Esta função é chamada internamente ao obter uma sessão.
    """
    try:
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }};
        """)
        session.set_keyspace(keyspace) # Conecta ao keyspace

        # Criação das tabelas
        session.execute("""
            CREATE TABLE IF NOT EXISTS filmes (
                titulo_id text PRIMARY KEY,
                titulo text,
                tipo text,
                ano_lancamento int,
                generos list<text>,
                nota float,
                numero_votos int,
                duracao int,
                sinopse text
            );
        """)
        # Adicionar um índice secundário em 'tipo' pode ser útil para algumas queries, mas com moderação.
        # session.execute("CREATE INDEX IF NOT EXISTS idx_filmes_tipo ON filmes (tipo);")

        session.execute("""
            CREATE TABLE IF NOT EXISTS atores (
                ator_id text PRIMARY KEY,
                nome_ator text,
                ano_nascimento int
            );
        """)
        # Adicionar um índice em nome_ator se for frequentemente buscado (USE COM CAUTELA - SASI PREFERÍVEL)
        # session.execute("CREATE INDEX IF NOT EXISTS idx_atores_nome_ator ON atores (nome_ator);")


        session.execute("""
            CREATE TABLE IF NOT EXISTS elenco (
                ator_id text,
                titulo_id text,
                nome_personagem text,
                PRIMARY KEY ((ator_id), titulo_id, nome_personagem) // ator_id como chave de partição
            );
        """)
        # Índices para buscas comuns no elenco, se necessário.
        # Ex: para buscar por titulo_id rapidamente (se não for parte da PK de forma eficiente)
        # session.execute("CREATE INDEX IF NOT EXISTS idx_elenco_titulo_id ON elenco (titulo_id);")

        print(f"Keyspace '{keyspace}' e tabelas verificadas/criadas com sucesso no Cassandra.")
    except Exception as e:
        # Em um app real, logar este erro criticamente.
        print(f"ALERTA: Falha ao criar/verificar schema no Cassandra para keyspace '{keyspace}': {e}")
        # Você pode decidir se quer levantar uma exceção aqui e parar a aplicação
        # ou se a aplicação pode tentar continuar (pode falhar depois).
        # Para o TCC, um print pode ser suficiente, mas o ideal é levantar o erro.
        # raise DatabaseInteractionError(f"Falha ao configurar schema do Cassandra: {e}") from e


def get_cassandra_session() -> Session:
    """Retorna uma sessão Cassandra conectada e com keyspace/tabelas garantidos."""
    try:
        auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, auth_provider=auth_provider)
        session = cluster.connect() # Conecta sem keyspace primeiro para criar o keyspace

        # Garante que o schema (keyspace e tabelas) exista
        _criar_schema_cassandra(session, CASSANDRA_KEYSPACE)
        
        # A função _criar_schema_cassandra já faz o session.set_keyspace()
        # Se não fizesse, você faria aqui: session.set_keyspace(CASSANDRA_KEYSPACE)
        
        return session
    except Exception as e:
        print(f"Falha crítica ao conectar ou configurar o Cassandra: {e}")
        # raise ConnectionError(f"Falha ao conectar no Cassandra: {e}") # Ou sua exceção customizada
        raise # Re-levanta a exceção original se for crítica para a conexão