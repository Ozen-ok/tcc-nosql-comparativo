from pydantic import BaseModel

# Modelo Pydantic para os dados de atores
class Ator(BaseModel):
    _id: str
    nome_ator: str
    ano_nascimento: int
    class Config:
        populate_by_name = True # Necessário para o alias funcionar na leitura
        # orm_mode = True # Para Pydantic V1, ou from_attributes = True para V2