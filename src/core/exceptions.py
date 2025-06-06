#src/core/exceptions.py
class DatabaseOperationError(Exception):
    """Classe base para erros de operação no banco de dados."""
    pass

class ItemNotFoundError(DatabaseOperationError):
    """Levantado quando um item não é encontrado no banco de dados."""
    pass

class ItemAlreadyExistsError(DatabaseOperationError):
    """Levantado quando um item que se tenta criar já existe."""
    pass

class DataValidationError(DatabaseOperationError):
    """Levantado quando os dados de entrada falham na validação."""
    def __init__(self, message="Erro na validação dos dados", errors: list = None):
        super().__init__(message)
        self.errors = errors if errors is not None else []

class DatabaseInteractionError(DatabaseOperationError):
    """Levantado para problemas gerais de interação com o banco de dados."""
    pass

class ValidationError(DatabaseOperationError):
    """Levantado para problemas gerais de interação com o banco de dados."""
    pass