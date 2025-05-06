from pydantic import BaseModel, model_validator
from typing import List

# Modelo Pydantic para os dados de filmes
class Movie(BaseModel):
    titulo_id: str
    nome_personagem: str
    tipo: str
    titulo: str
    ano_lancamento: int
    generos: List[str]
    nome_ator: str
    ano_nascimento: int
    nota: float
    numero_votos: int
    duracao: float
    sinopse: str

    class Config:
        from_attributes = True  # Alterado de orm_mode para from_attributes na versão atual do Pydantic

# Modelo Pydantic para os dados de atores
class Actor(BaseModel):
    ator_id: str
    nome_ator: str
    ano_nascimento: int
