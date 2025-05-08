from pydantic import BaseModel
from typing import List, Optional

# Modelo Pydantic para os dados de filmes
class Filme(BaseModel):
    titulo_id: str
    titulo: str
    tipo: str
    ano_lancamento: int
    generos: List[str]
    nota: float
    numero_votos: int
    duracao: float
    sinopse: str

    # Não há necessidade de nome_personagem, tipo, nome_ator ou ano_nascimento aqui
    # Esses campos serão tratados na inserção das relações de elenco (Ator-Filme)

    class Config:
        from_attributes = True

# Modelo Pydantic para os dados de atores
class Ator(BaseModel):
    ator_id: str
    nome_ator: str
    ano_nascimento: int

# Modelo Pydantic para as relações de elenco (Ator - Filme)
class Elenco(BaseModel):
    ator_id: str
    titulo_id: str
    nome_personagem: str
