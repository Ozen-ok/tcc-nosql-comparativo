from pydantic import BaseModel

# Modelo Pydantic para as relações de elenco (Ator - Filme)
class Elenco(BaseModel):
    ator_id: str
    titulo_id: str
    nome_personagem: str
