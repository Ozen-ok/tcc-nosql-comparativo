from pydantic import BaseModel
from typing import List

# Modelo Pydantic para os dados de filmes
class Filme(BaseModel):
    _id: str
    titulo: str
    tipo: str
    ano_lancamento: int
    generos: List[str]
    nota: float
    numero_votos: int
    duracao: int
    sinopse: str

    # Não há necessidade de nome_personagem, nome_ator ou ano_nascimento aqui
    # Esses campos serão tratados na inserção das relações de elenco (Ator-Filme)

    class Config:
        populate_by_name = True # Necessário para o alias funcionar na leitura
        from_attributes = True