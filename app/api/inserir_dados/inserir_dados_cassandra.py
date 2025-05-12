from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import csv
import ast
from config.db_config import get_cassandra_db

# Conexão com o Cassandra
session = get_cassandra_db()

# Criação da keyspace
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS imdb WITH replication = {
        'class': 'SimpleStrategy',
        'replication_factor': 1
    };
""")

# Selecionar a keyspace
session.set_keyspace(CASSANDRA_KEYSPACE)

# Criação das tabelas

# Tabela de filmes
session.execute("""
    CREATE TABLE IF NOT EXISTS filmes (
        titulo_id text PRIMARY KEY,
        titulo text,
        tipo text,
        ano_lancamento int,
        generos list<text>,
        nota float,
        numero_votos int,
        duracao float,
        sinopse text
    );
""")

# Tabela de atores
session.execute("""
    CREATE TABLE IF NOT EXISTS atores (
        ator_id text PRIMARY KEY,
        nome_ator text,
        ano_nascimento int
    );
""")

# Tabela de elenco
session.execute("""
    CREATE TABLE IF NOT EXISTS elenco (
        ator_id text,
        titulo_id text,
        nome_personagem text,
        PRIMARY KEY (ator_id, titulo_id)
    );
""")

# Inserção de dados dos arquivos TSV

# Filmes
with open('data/filmes.tsv', newline='', encoding='utf-8') as tsvfile:
    reader = csv.DictReader(tsvfile, delimiter='\t')
    for row in reader:
        generos_list = ast.literal_eval(row['generos']) if row['generos'] else []
        session.execute("""
            INSERT INTO filmes (
                titulo_id, titulo, tipo, ano_lancamento, generos,
                nota, numero_votos, duracao, sinopse
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['titulo_id'],
            row['titulo'],
            row['tipo'],
            int(row['ano_lancamento']) if row['ano_lancamento'] else None,
            generos_list,
            float(row['nota']) if row['nota'] else None,
            int(row['numero_votos']) if row['numero_votos'] else None,
            float(row['duracao']) if row['duracao'] else None,
            row['sinopse']
        ))

# Atores
with open('data/atores.tsv', newline='', encoding='utf-8') as tsvfile:
    reader = csv.DictReader(tsvfile, delimiter='\t')
    for row in reader:
        session.execute("""
            INSERT INTO atores (
                ator_id, nome_ator, ano_nascimento
            ) VALUES (%s, %s, %s)
        """, (
            row['ator_id'],
            row['nome_ator'],
            int(row['ano_nascimento']) if row['ano_nascimento'] else None
        ))

# Elenco
with open('data/elenco.tsv', newline='', encoding='utf-8') as tsvfile:
    reader = csv.DictReader(tsvfile, delimiter='\t')
    for row in reader:
        session.execute("""
            INSERT INTO elenco (
                ator_id, titulo_id, nome_personagem
            ) VALUES (%s, %s, %s)
        """, (
            row['ator_id'],
            row['titulo_id'],
            row['nome_personagem']
        ))

print("✅ Dados inseridos com sucesso no Cassandra.")
