# src/models/api_models.py
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

class FiltrosBuscaAvancadaPayload(BaseModel):
    titulo: Optional[str] = None
    tipo: Optional[str] = None
    ano_lancamento_min: Optional[int] = None
    generos: Optional[List[str]] = None
    nota_min: Optional[float] = None
    duracao_min: Optional[int] = None
    ordenar_por: Optional[str] = "nota"
    ordem: Optional[int] = -1
    class Config: from_attributes = True

class FilmePayload(BaseModel): # Usado para Inserir e pode ser base para Atualizar
    titulo_id: str # Usaremos para popular o _id no MongoDB
    titulo: str
    tipo: str
    ano_lancamento: int
    generos: List[str]
    nota: float
    numero_votos: int
    duracao: int
    sinopse: str
    class Config: from_attributes = True

class AtualizarFilmePayload(BaseModel):
    """Payload para atualizar campos específicos de um filme."""
    # Permitir que qualquer campo de FilmePayload (exceto titulo_id talvez) seja opcional
    # ou defina campos específicos que podem ser atualizados.
    # Exemplo mais simples: um dicionário genérico para o campo e valor.
    # Uma abordagem mais tipada seria ter todos os campos de Filme como Optional.
    campo: str = Field(..., description="O nome do campo do filme a ser atualizado.")
    novo_valor: Any = Field(..., description="O novo valor para o campo.")
    # Exemplo mais tipado (se você quiser atualizar múltiplos campos de uma vez):
    # titulo: Optional[str] = None
    # tipo: Optional[str] = None
    # ano_lancamento: Optional[int] = None
    # generos: Optional[List[str]] = None
    # nota: Optional[float] = None
    # numero_votos: Optional[int] = None
    # duracao: Optional[int] = None
    # sinopse: Optional[str] = None
    class Config: from_attributes = True


class CarregarBasePayload(BaseModel):
    filmes_path: str
    atores_path: str
    elenco_path: str

# --- Modelos para Respostas (Exemplos, se quiser tipar as respostas da API) ---
class FilmeResponse(BaseModel): # Similar ao seu src/models/filme.py, mas usando 'id'
    id: str = Field(alias="_id") # Se _id é o seu titulo_id (string)
    titulo: Optional[str] = None
    tipo: Optional[str] = None
    ano_lancamento: Optional[int] = None
    generos: Optional[List[str]] = None
    nota: Optional[float] = None
    numero_votos: Optional[int] = None
    duracao: Optional[int] = None
    sinopse: Optional[str] = None
    class Config:
        from_attributes = True
        populate_by_name = True

class AtorResponse(BaseModel): # Similar ao seu src/models/ator.py
    id: str = Field(alias="_id") # Se _id é o seu ator_id (string)
    nome_ator: Optional[str] = None
    ano_nascimento: Optional[int] = None
    # Se buscar_atores_por_filmes retornar mais campos, adicione aqui
    nome_personagem: Optional[str] = None
    outros_filmes: Optional[List[Dict[str, Any]]] = None # Lista de filmes simplificados
    class Config:
        from_attributes = True
        populate_by_name = True

class ContagemPorAnoResponse(BaseModel):
    # O _id da agregação é o ano
    ano: Any = Field(alias="_id") # O _id pode ser int ou str dependendo do dado original
    quantidade: int
    class Config:
        from_attributes = True
        populate_by_name = True

class MediaGeneroResponse(BaseModel):
    genero: str
    media_nota: float
    class Config: from_attributes = True

class OperacaoStatusResponse(BaseModel):
    status: str
    message: str
    details: Optional[Any] = None