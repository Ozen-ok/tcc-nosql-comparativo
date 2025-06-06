# src/streamlit_app/services/operation_handlers.py
from . import api_service # Usando . para import relativo
from typing import Any, Dict, List, Optional

# Exemplo para busca avançada
def realizar_busca_avancada(filtros_da_ui: dict, banco_selecionado: str):
    # Aqui você pode adicionar lógicas de transformação de dados da UI para API, se necessário
    # Por exemplo, converter uma string de gêneros "Ação, Comédia" para uma lista ["Ação", "Comédia"]
    # if isinstance(filtros_da_ui.get("generos"), str):
    #     filtros_da_ui["generos"] = [g.strip() for g in filtros_da_ui["generos"].split(",")]

    return api_service.buscar_filmes_avancado(
        filtros_busca=filtros_da_ui,
        banco_alvo=banco_selecionado
    )

def carregar_base_de_dados_handler(filmes_path: str, atores_path: str, elenco_path: str, banco_selecionado: str):
    return api_service.carregar_base_completa(filmes_path, atores_path, elenco_path, banco_alvo=banco_selecionado)

def obter_detalhes_filme_handler(id_filme: str, banco_selecionado: str):
    return api_service.buscar_filme_por_id(id_filme, banco_alvo=banco_selecionado)

def obter_atores_do_filme_handler(id_filme: str, banco_selecionado: str):
    return api_service.listar_atores_de_filme(id_filme, banco_alvo=banco_selecionado)

# ... e assim por diante para todas as operações (inserir, atualizar, deletar, contagens, médias)
# Cada função aqui chama a função correspondente em api_service.py, passando o banco_selecionado.

def handle_atualizar_filme_operacao(id_filme: str, campo: str, novo_valor: Any, banco_selecionado: str):
    # Validações ou transformações podem ocorrer aqui se necessário
    return api_service.atualizar_filme_existente(id_filme, campo, novo_valor, banco_selecionado)

def handle_remover_filme_operacao(id_filme: str, banco_selecionado: str):
    return api_service.remover_filme_por_id(id_filme, banco_alvo=banco_selecionado)

def handle_listar_filmes_por_ator_operacao(
    id_ator: str, 
    banco_selecionado: str,
    ordenar_por: Optional[str] = "nota", # Defaults podem ser definidos aqui ou na UI
    ordem: Optional[int] = -1,
    limite: Optional[int] = 50
):
    # Se a UI sempre fornecer ID, ótimo. Se fornecer nome, este handler
    # poderia chamar um outro api_service para buscar o ID do ator pelo nome primeiro,
    # ou o backend lida com a busca por nome.
    return api_service.listar_filmes_por_ator_api(id_ator, banco_selecionado, ordenar_por, ordem, limite)

def handle_contar_filmes_por_ano_operacao(banco_selecionado: str):
    return api_service.contar_filmes_por_ano(banco_alvo=banco_selecionado)

def handle_media_notas_por_genero_operacao(banco_selecionado: str):
    return api_service.calcular_media_notas_por_genero(banco_alvo=banco_selecionado)

# src/streamlit_app/services/operation_handlers.py
from . import api_service 
# ... (outros handlers como realizar_busca_avancada, carregar_base_de_dados_handler, etc.)

def handle_inserir_filme_operacao(dados_filme: dict, banco_selecionado: str):
    """
    Handler para a operação de inserir um novo filme.
    Chama o serviço de API correspondente.
    """
    # Validações ou transformações adicionais nos dados_filme podem ser feitas aqui antes de enviar para a API, se necessário.
    # Por exemplo, garantir que 'nota' seja float, 'ano_lancamento' seja int, etc.
    # Mas o Pydantic no backend já fará a validação final.
    return api_service.inserir_novo_filme_api( # Nome da função no api_service.py
        dados_filme=dados_filme,
        banco_alvo=banco_selecionado
    )

# ... (outros handlers que você já tem ou vai adicionar)