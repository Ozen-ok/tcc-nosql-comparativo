# src/streamlit_app/components/display_utils.py
import streamlit as st
import pandas as pd
import os
import plotly.express as px
from typing import Dict, Any, List, Union # Adicione Union aqui
from PIL import Image # Certifique-se que Pillow está instalado
from .navigation import paginar_resultados # Supondo que navigation.py está em components
from config.settings import BANCOS_SUPORTADOS # Importa o mapeamento de bancos
# src/streamlit_app/components/display_utils.py

def _preparar_filme_para_exibicao(filme_dict: dict) -> dict:
    """Adiciona URL do poster e trata campos para exibição."""
    filme_copy = filme_dict.copy()
    # Se você padronizou para 'id' com alias para '_id' nos modelos Pydantic de resposta,
    # e o CRUD retorna o campo como '_id', o filme_copy.get('_id', '') ainda funciona.
    # Se o CRUD já retorna 'id', então use filme_copy.get('id', '')
    id_do_filme = filme_copy.get('_id', filme_copy.get('id', '')) # Pega _id ou id
    filme_copy["poster_url"] = f"assets/imagens/posters/{id_do_filme}.jpg"
    
    if 'nota' in filme_copy and filme_copy['nota'] is not None:
        try:
            filme_copy['nota_formatada'] = f"{float(filme_copy['nota']):.1f}"
        except (ValueError, TypeError):
            filme_copy['nota_formatada'] = "N/A"
    else:
        filme_copy['nota_formatada'] = "N/A"

    if 'numero_votos' in filme_copy and filme_copy['numero_votos'] is not None:
        try:
            filme_copy['votos_formatados'] = f"{int(filme_copy['numero_votos']):,}"
        except (ValueError, TypeError):
            filme_copy['votos_formatados'] = "N/A"
    else:
        filme_copy['votos_formatados'] = "N/A"
    return filme_copy

# src/streamlit_app/components/display_utils.py
import streamlit as st
import os
from PIL import Image, ImageOps # Adicione ImageOps para a função fit se quiser usá-la
from typing import Dict # Adicione se não estiver lá
# ... (outros imports que você já tem) ...

# Suas funções _preparar_filme_para_exibicao, etc.

def exibir_cartao_filme(filme: dict, col_streamlit=None, pagina_origem_path="paginas/busca_avancada.py", pagina_origem_label="Busca Avançada"):
    """Exibe um único cartão de filme com botão para detalhes e imagem padronizada."""
    container = col_streamlit if col_streamlit else st
    filme_display = _preparar_filme_para_exibicao(filme) # _preparar_filme_para_exibicao já retorna poster_url

    id_filme_para_key = filme_display.get('_id', filme_display.get('id', 'filme_sem_id'))
    banco_origem_para_key = filme_display.get('banco_origem', 'banco_desconhecido')
    #banco_origem_para_key = banco_nome_amigavel
    
    poster_path = filme_display.get("poster_url", "")

    # --- LÓGICA DE PROCESSAMENTO DA IMAGEM ---
    TARGET_POSTER_WIDTH = 280  # Defina a largura desejada
    TARGET_POSTER_HEIGHT = 420 # Defina a altura desejada (para um aspect ratio ~2:3)

    if os.path.exists(poster_path):
        try:
            img = Image.open(poster_path)
            
            # Usando ImageOps.fit para redimensionar e cortar mantendo o centro
            # Esta função redimensiona a imagem para preencher as dimensões e corta o excesso.
            # Image.Resampling.LANCZOS é um filtro de alta qualidade para redimensionamento.
            img_processada = ImageOps.fit(img, (TARGET_POSTER_WIDTH, TARGET_POSTER_HEIGHT), Image.Resampling.LANCZOS)
            
            container.image(img_processada, width=TARGET_POSTER_WIDTH) # Exibe a imagem processada
        except Exception as e:
            container.caption(f"Erro ao carregar/processar poster: {id_filme_para_key}")
            print(f"Erro imagem {poster_path}: {e}") 
    else:
        # Você pode criar uma imagem placeholder de tamanho padrão aqui se quiser
        # Ex: placeholder = Image.new('RGB', (TARGET_POSTER_WIDTH, TARGET_POSTER_HEIGHT), color = 'grey')
        # container.image(placeholder, caption=f"Poster não disponível: {id_filme_para_key}")
        container.caption(f"Poster não disponível: {id_filme_para_key}")
        # Para manter a altura, você pode adicionar um st.empty() com altura definida via markdown (hack CSS)
        # ou simplesmente aceitar que cartões sem imagem podem ser menores.
        # Exemplo simples de espaço reservado (não garante altura exata sem CSS):
        container.markdown(f"<div style='height:{TARGET_POSTER_HEIGHT}px; display:flex; align-items:center; justify-content:center; background-color:#f0f0f0; color:#ccc; font-size:smaller;'>Poster Indisponível</div>", unsafe_allow_html=True)


    # --- O restante do conteúdo do cartão continua igual ---
    container.subheader(f"{filme_display.get('titulo', 'N/A')} ({int(filme_display.get('ano_lancamento', 0))})")
    if filme_display.get("nota", 0) == 0: # Considera 0 como não lançado/sem nota
        container.markdown("⭐ Ainda não lançado | 🗳️ Votos indisponíveis")
    else:
        container.markdown(f"⭐ {filme_display.get('nota_formatada', 'N/A')} | 🗳️ {filme_display.get('votos_formatados', 'N/A')} votos")
    
    if filme_display.get("tipo", "").lower() != "jogo" and  filme_display.get("nota", 0) != 0:# Não mostra duração para jogos
         duracao_filme = filme_display.get('duracao')
         container.markdown(f"⏱️ {duracao_filme if duracao_filme else 'N/A'} min.")
    
    generos_exibir = filme_display.get('generos', [])
    container.markdown(f"🎞️ {', '.join(generos_exibir) if generos_exibir else 'Gênero N/A'}")

    def callback_ver_detalhes():
        # Mostra o toast assim que o botão é clicado!
        st.toast(f"Abrindo detalhes de {filme_display.get('titulo', 'filme')}...", icon="🎬") # Ícone opcional
        
        # O resto do seu callback continua igual:
        st.session_state['filme_id_selecionado'] = filme_display.get('_id', filme_display.get('id'))
        chave_banco_para_detalhes = filme_display.get('banco_origem', st.session_state.get('banco_em_uso_na_pagina_key', 'mongo'))
        #chave_banco_para_detalhes = banco_chave_selecionada
        st.session_state['banco_filme_selecionado'] = chave_banco_para_detalhes
        st.session_state['pagina_anterior_detalhes'] = pagina_origem_path
        st.session_state['label_pagina_anterior_detalhes'] = pagina_origem_label
        st.session_state['navegar_para_detalhes'] = True 
    
    id_filme_para_key = filme_display.get('_id', filme_display.get('id', f"filme_sem_id_{os.urandom(4).hex()}"))
    banco_origem_para_key = filme_display.get('banco_origem', 'banco_desconhecido')
    
    # Key do botão precisa ser bem única para evitar conflitos
    unique_button_key = f"details_btn_{id_filme_para_key}_{banco_origem_para_key}_{os.urandom(4).hex()}"

    if container.button("🔍 Ver Detalhes", key=unique_button_key, on_click=callback_ver_detalhes):
        # O st.switch_page será acionado no rerun, pelo flag 'navegar_para_detalhes'
        # que é checado no topo das páginas de busca.
        pass


def exibir_lista_de_filmes(
    lista_filmes: list,
    banco_chave_selecionada: str,
    resultados_por_linha: int = 3,
    # Adicione estes parâmetros para saber de onde estamos vindo
    pagina_atual_path_para_voltar: str = "paginas/busca_avancada.py", # Default
    pagina_atual_label_para_voltar: str = "Busca Avançada" # Default
): 
    banco_nome_amigavel = BANCOS_SUPORTADOS.get(banco_chave_selecionada, banco_chave_selecionada.capitalize())
    
    """Exibe uma lista de filmes em cartões, organizados em colunas."""
    if not lista_filmes:
        st.info(f"Nenhum filme encontrado para os critérios no banco: {banco_nome_amigavel}.")
        return

    st.markdown(f"### Resultados em {banco_nome_amigavel}")

    chave_paginacao = f"paginacao_{banco_nome_amigavel.replace(' ', '_').lower()}"
    filmes_paginados = paginar_resultados(lista_filmes, tamanho_pagina=9, chave=chave_paginacao)

    num_cols = resultados_por_linha
    
    for i in range(0, len(filmes_paginados), num_cols):
        cols = st.columns(num_cols) # Cria colunas para cada linha de itens
        for j in range(num_cols):
            if i + j < len(filmes_paginados):
                filme_data = filmes_paginados[i+j]
                # Passa os parâmetros de origem para o cartão
                exibir_cartao_filme(
                    filme_data,
                    cols[j],
                    pagina_origem_path=pagina_atual_path_para_voltar,
                    pagina_origem_label=pagina_atual_label_para_voltar,
                )
    st.markdown("---")

def processar_e_exibir_resposta_escrita_api(
    resposta_api_completa: Dict[str, Any], 
    banco_selecionado_chave: str, # "mongo", "todos", etc.
    operacao_label: str # Ex: "Atualização de Filme"
):
    if not isinstance(resposta_api_completa, dict):
        st.error(f"Resposta da API para '{operacao_label}' em formato inesperado.")
        st.json(resposta_api_completa)
        return

    # Checa erro global da API primeiro
    if "error" in resposta_api_completa and "status" not in resposta_api_completa :
        st.error(f"Falha na {operacao_label.lower()}: {resposta_api_completa['error']}")
        return
    
    if resposta_api_completa.get("status") != "sucesso":
        st.warning(f"API não retornou status 'sucesso' para {operacao_label.lower()}. Mensagem: {resposta_api_completa.get('mensagem', 'N/A')}")
        # Mesmo assim, pode haver detalhes em 'dados' se for modo 'todos'

    # O endpoint, quando bem-sucedido, envelopa o resultado em "dados" via resposta_sucesso.
    dados_retornados_pela_api = resposta_api_completa.get("dados")

    if dados_retornados_pela_api is None and resposta_api_completa.get("status") == "sucesso":
        # Isso pode acontecer se a resposta_sucesso foi chamada sem 'dados' payload
        # Ex: para uma deleção bem-sucedida que só retorna mensagem.
        st.success(f"✅ {BANCOS_SUPORTADOS.get(banco_selecionado_chave, banco_selecionado_chave.capitalize())}: {resposta_api_completa.get('mensagem', operacao_label + ' realizada.')}")
        return

    if not isinstance(dados_retornados_pela_api, dict):
        st.error(f"Estrutura de 'dados' inesperada na resposta da API para {operacao_label.lower()}.")
        st.json(resposta_api_completa)
        return

    if banco_selecionado_chave != "todos":
        # Para banco único, dados_retornados_pela_api é o filme atualizado (se for AtualizarFilme)
        # ou o dict de mensagem (se for RemoverFilme, já tratado acima se 'dados' for None).
        # O endpoint de atualização para banco único agora retorna o FilmeResponse diretamente (não envelopado por resposta_sucesso).
        # E o de remoção retorna OperacaoStatusResponse.
        # Esta função `processar_e_exibir_resposta_escrita_api` precisa ser mais específica
        # ou a `processar_e_exibir_resposta_simples_api` ser adaptada.

        # Vamos reusar a lógica da `processar_e_exibir_resposta_simples_api` que você já tem.
        # Ela espera que `resposta_api` seja o dict que contém "data" (filme) ou "message".
        # Se o endpoint de banco único retorna o FilmeResponse diretamente,
        # e o de remoção retorna OperacaoStatusResponse, a `processar_e_exibir_resposta_simples_api`
        # precisaria ser chamada com `resposta_api_completa` diretamente.
        
        # Por agora, vamos focar no caso "todos". O caso de banco único
        # para escrita já deve estar sendo tratado pela `processar_e_exibir_resposta_simples_api`
        # se o endpoint retornar o formato que ela espera.
        # Se o endpoint de atualização para banco único retorna o FilmeResponse diretamente (não um dict com 'data'),
        # então a página do Streamlit que chama isso precisa tratar de forma diferente.
        # VOU ASSUMIR QUE O CASO DE BANCO ÚNICO JÁ ESTÁ SENDO TRATADO ADEQUADAMENTE PELA SUA UI.
        # O FOCO AQUI É O MODO "TODOS".
         nome_banco_amigavel = BANCOS_SUPORTADOS.get(banco_selecionado_chave, banco_selecionado_chave.capitalize())
         st.info(f"Resultado para {operacao_label} em {nome_banco_amigavel}:")
         # Se o endpoint de atualização para banco único agora retorna FilmeResponse direto,
         # e o de remoção OperacaoStatusResponse, `resposta_api_completa` JÁ É esse objeto.
         # O display_utils precisaria de funções para exibir esses objetos Pydantic.
         # Por ora, vamos exibir o JSON.
         st.json(resposta_api_completa)


    else: # Modo "todos"
        st.subheader(f"{operacao_label} (Status por Banco)")
        # dados_retornados_pela_api é o dict {"mongo": {"data": ..., "message": ...} ou {"error": ...}, ...}
        
        for nome_banco_int, res_banco in dados_retornados_pela_api.items():
            if nome_banco_int not in BANCOS_SUPORTADOS: continue

            nome_banco_amigavel_loop = BANCOS_SUPORTADOS.get(nome_banco_int, nome_banco_int.capitalize())
            with st.expander(f"Resultado para {nome_banco_amigavel_loop}:", expanded=True):
                if isinstance(res_banco, dict):
                    if "error" in res_banco:
                        st.error(f"❌ Falha: {res_banco['error']} (Status: {res_banco.get('status_code', 'N/A')})")
                    elif "data" in res_banco and res_banco["data"] is not None: # Para atualização, data é o filme
                        st.success(f"✅ {res_banco.get('message', operacao_label + ' bem-sucedida!')}")
                        st.caption("Dados retornados:")
                        st.json(res_banco["data"])
                    elif "message" in res_banco: # Para remoção bem-sucedida
                        st.success(f"✅ {res_banco['message']}")
                    else:
                        st.info("Resposta do banco sem detalhes claros.")
                        st.json(res_banco)
                else:
                    st.warning(f"Formato de resposta inesperado do {nome_banco_amigavel_loop}.")
                    st.json(res_banco)
        st.markdown("---")


def processar_e_exibir_resposta_simples_api(resposta_api: dict, nome_banco_amigavel: str, operacao: str = "Operação"):
    """
    Processa e exibe uma resposta simples da API (ex: para inserção, atualização, deleção).
    Espera uma resposta que pode ser o objeto criado/modificado ou uma mensagem de status/erro.
    """
    if not isinstance(resposta_api, dict):
        st.error(f" Resposta inesperada do servidor para {operacao.lower()}.")
        return

    if "error" in resposta_api:
        st.error(f"❌ Falha na {operacao.lower()}: {resposta_api['error']} (Status: {resposta_api.get('status_code', 'N/A')})")
    elif "data" in resposta_api and isinstance(resposta_api["data"], dict): # Se o backend envelopar em 'data'
        st.success(f"✅ {resposta_api.get('message', operacao + ' realizada com sucesso!')}")
        with st.expander("Detalhes do item processado:", expanded=False):
            st.json(resposta_api["data"]) # Mostra o filme inserido, por exemplo
    elif "message" in resposta_api: # Se for apenas uma mensagem de status
         st.success(f"✅ {resposta_api['message']}")
    else: # Formato não reconhecido, mas não é um erro explícito
        st.info(f"{operacao} processada. Resposta do servidor:")
        st.json(resposta_api)

def processar_e_exibir_resultados_api(
    resposta_api: Any, # Alterado para Any para permitir lista ou dict
    banco_nome_key: str = "mongo",
    pagina_atual_path_para_voltar: str = "paginas/busca_avancada.py",
    pagina_atual_label_para_voltar: str = "Busca Avançada"
):
    """
    Processa a resposta da API e exibe os filmes.
    A resposta_api pode ser uma lista de filmes (para banco único)
    ou um dicionário com resultados por banco (para 'todos').
    """

    if not resposta_api: # Se for None ou lista/dict vazio
        st.info("Nenhum dado retornado pela API.")
        return

    if isinstance(resposta_api, dict) and "error" in resposta_api:
        st.error(f"Falha ao buscar dados: {resposta_api['error']} (Status: {resposta_api.get('status_code', 'N/A')})")
        return

    # CASO 1: A resposta_api JÁ É UMA LISTA DE FILMES (ex: de listar_filmes_por_ator para um banco)
    if isinstance(resposta_api, list):
        #banco_em_uso = st.session_state.get('banco_em_uso_na_pagina', 'Resultados') # Nome amigável do banco
        # Adiciona 'banco_origem' se não vier da API para cada filme (pode ser útil para o botão de detalhes)
        # Se o st.session_state.get('banco_chave_selecionada_busca') ou similar estiver disponível e for a chave interna
        #chave_banco_atual = st.session_state.get('banco_selecionado_filmes_ator', # Ou a chave relevante da página
        #
        print("caso 1")                              #st.session_state.get('banco_chave_selecionada_busca', 'desconhecido'))

        filmes_com_origem = []
        for filme in resposta_api:
            if isinstance(filme, dict): # Garante que é um dicionário
                filme_copy = filme.copy()
                filme_copy['banco_origem'] = banco_nome_key # Adiciona a chave interna do banco
                filmes_com_origem.append(filme_copy)
            else:
                st.warning(f"Item inesperado na lista de filmes: {filme}") # Avisa se algo não for dict
          
        exibir_lista_de_filmes(
            filmes_com_origem, # Passa a lista de filmes com a origem do banco
            banco_nome_key,
            pagina_atual_path_para_voltar=pagina_atual_path_para_voltar,
            pagina_atual_label_para_voltar=pagina_atual_label_para_voltar
        )
        return # Encerra aqui se já processou a lista

    # CASO 2: A resposta_api é um DICIONÁRIO (pode ser para 'todos' os bancos ou um único banco aninhado)
    if isinstance(resposta_api, dict):

        # Se for resultado de um único banco, mas aninhado em 'data' (menos provável com os ajustes recentes)
        if "data" in resposta_api and isinstance(resposta_api["data"], list) and \
           not any(banco_key in resposta_api for banco_key in BANCOS_SUPORTADOS.keys() if banco_key != "data"): # Garante que não é o formato de 'todos'
            #banco_em_uso = st.session_state.get('banco_em_uso_na_pagina', 'Resultado')
            print("caso 2")
            #chave_banco_atual = st.session_state.get('banco_selecionado_filmes_ator',
            #st.session_state.get('banco_chave_selecionada_busca', 'desconhecido'))
            
            filmes_com_origem = []
            for filme in resposta_api["data"]:
                if isinstance(filme, dict):
                    filme_copy = filme.copy()
                    filme_copy['banco_origem'] = banco_nome_key
                    filmes_com_origem.append(filme_copy)

            exibir_lista_de_filmes(
                filmes_com_origem,
                banco_nome_key,
                pagina_atual_path_para_voltar=pagina_atual_path_para_voltar,
                pagina_atual_label_para_voltar=pagina_atual_label_para_voltar
            )
            return
        
        print("caso 3")

        # Se for um dicionário com chaves sendo os nomes dos bancos (para 'todos')
        bancos_processados = 0
        for nome_banco_chave, resultado_banco in resposta_api.items():
            if nome_banco_chave in BANCOS_SUPORTADOS and isinstance(resultado_banco, dict):
                if "error" in resultado_banco:
                    st.error(f"Erro no {BANCOS_SUPORTADOS.get(nome_banco_chave, nome_banco_chave)}: {resultado_banco['error']}")
                    bancos_processados +=1 # Conta como processado para não cair no "formato não reconhecido"
                    continue
                
                dados_filmes = resultado_banco.get("data", resultado_banco.get("filmes"))
                if isinstance(dados_filmes, list):
                    filmes_com_origem_loop = []
                    for filme in dados_filmes:
                        if isinstance(filme, dict):
                            filme_copy = filme.copy()
                            filme_copy['banco_origem'] = nome_banco_chave # Usa a chave do banco do loop
                            filmes_com_origem_loop.append(filme_copy)
                    
                    exibir_lista_de_filmes(
                        filmes_com_origem_loop,
                        nome_banco_chave,
                        pagina_atual_path_para_voltar=pagina_atual_path_para_voltar,
                        pagina_atual_label_para_voltar=pagina_atual_label_para_voltar
                    )
                    bancos_processados += 1
                elif resultado_banco.get("message"): # Mensagem de sucesso sem lista de filmes
                    st.success(f"{BANCOS_SUPORTADOS.get(nome_banco_chave, nome_banco_chave)}: {resultado_banco['message']}")
                    bancos_processados += 1
            elif nome_banco_chave == "message" and isinstance(resultado_banco, str) and len(resposta_api.keys()) == 1:
                 # Caso especial: a API pode ter retornado só uma mensagem geral (ex: de uma operação de escrita)
                 st.info(resultado_banco)
                 return


        if bancos_processados == 0 and not ("message" in resposta_api and len(resposta_api.keys()) == 1) : # Se nada foi processado
             st.info("Nenhum dado para exibir ou formato de resposta não reconhecido.")
        return

    # Fallback se não for nem dict nem list (improvável se _make_request funcionar)
    st.error(f"Formato de resposta da API completamente inesperado: {type(resposta_api)}")


def _exibir_resultado_unico_carga_banco(resultado_banco_especifico: dict, nome_banco_amigavel: str):
    """Exibe o resultado da carga para um banco de dados específico."""
    if not isinstance(resultado_banco_especifico, dict):
        st.error(f"Formato de resposta inesperado para {nome_banco_amigavel}.")
        return

    status_op = resultado_banco_especifico.get("status", "desconhecido")
    message = resultado_banco_especifico.get("message", "Nenhuma mensagem retornada.")
    detalhes_carga = resultado_banco_especifico.get("detalhes_carga")
    erros_execucao = resultado_banco_especifico.get("erros_execucao", [])

    if status_op == "sucesso":
        st.success(f"✅ {message}")
    elif status_op == "concluído_com_erros":
        st.warning(f"⚠️ {message}")
    else: # erro ou desconhecido
        st.error(f"❌ Falha na operação. {message}")

    if detalhes_carga:
        md_detalhes = "- Filmes Processados/Inseridos: {}\n".format(detalhes_carga.get('filmes', 'N/A') if isinstance(detalhes_carga.get('filmes'), int) else detalhes_carga.get('filmes_inseridos', 'N/A'))
        md_detalhes += "- Atores Processados/Inseridos: {}\n".format(detalhes_carga.get('atores', 'N/A') if isinstance(detalhes_carga.get('atores'), int) else detalhes_carga.get('atores_inseridos', 'N/A'))
        md_detalhes += "- Elenco Processado/Inserido: {}".format(detalhes_carga.get('elenco', 'N/A') if isinstance(detalhes_carga.get('elenco'), int) else detalhes_carga.get('elenco_inserido', 'N/A'))
        with st.expander(f"Detalhes da Carga em {nome_banco_amigavel}", expanded=False):
            st.markdown(md_detalhes)

    if erros_execucao:
        st.error(f"Encontrados {len(erros_execucao)} erro(s) durante a carga em {nome_banco_amigavel}:")
        # Limitar a exibição de erros para não poluir demais a tela
        max_erros_display = 5 
        for i, erro_item in enumerate(erros_execucao[:max_erros_display]):
            exp_title = f"Erro {i+1}: "
            exp_title += f"Arq: {erro_item.get('arq','N/A')}, " if erro_item.get('arq') else ""
            exp_title += f"Linha: {erro_item.get('ln','N/A')}, " if erro_item.get('ln') else ""
            exp_title += f"ID: {erro_item.get('id_problematico') or erro_item.get('ids_problematicos','N/A')}, " if erro_item.get('id_problematico') or erro_item.get('ids_problematicos') else ""
            exp_title += f"Tipo: {erro_item.get('tipo_erro') or erro_item.get('err', 'N/A')}"
            
            with st.expander(exp_title, expanded=False):
                st.json(erro_item.get("detalhes", erro_item)) # Mostra o erro completo no expander
        if len(erros_execucao) > max_erros_display:
            st.warning(f"... e mais {len(erros_execucao) - max_erros_display} erro(s). Verifique os logs do servidor para detalhes completos.")
    st.markdown("---")

def processar_e_exibir_resultados_carga_api(resposta_api_geral: dict):
    """
    Processa a resposta da API para a operação de carga de base,
    que pode conter resultados de um ou múltiplos bancos.
    """
    if not isinstance(resposta_api_geral, dict):
        st.error("Resposta inesperada do servidor para a operação de carga.")
        return

    # Verifica se é um erro global da API antes de tentar iterar por bancos
    if "error" in resposta_api_geral and not any(banco_key in resposta_api_geral for banco_key in BANCOS_SUPORTADOS.keys()):
        st.error(f"Falha na operação de carga da API: {resposta_api_geral['error']} (Status: {resposta_api_geral.get('status_code', 'N/A')})")
        return

    # Se a resposta_api_geral já é o resultado de um único banco (porque o serviço retornou assim)
    # E não tem chaves de banco como 'mongo', 'cassandra' etc.
    if "status" in resposta_api_geral and ("message" in resposta_api_geral or "erros_execucao" in resposta_api_geral):
        banco_em_uso_label = st.session_state.get('banco_em_uso_na_pagina_carga', 'Resultado da Carga')
        _exibir_resultado_unico_carga_banco(resposta_api_geral, banco_em_uso_label)
    else: # Se for um dicionário com chaves sendo os nomes dos bancos
        bancos_retornados = 0
        for nome_banco_chave, resultado_banco_especifico in resposta_api_geral.items():
            if nome_banco_chave in BANCOS_SUPORTADOS: # Verifica se é uma chave de banco válida
                nome_banco_amigavel = BANCOS_SUPORTADOS.get(nome_banco_chave, nome_banco_chave.capitalize())
                _exibir_resultado_unico_carga_banco(resultado_banco_especifico, nome_banco_amigavel)
                bancos_retornados += 1
            elif nome_banco_chave == "error" and isinstance(resultado_banco_especifico, str): # Um erro geral retornado pela API para a chamada "todos"
                st.error(f"Erro geral na API ao processar para 'todos': {resultado_banco_especifico}")
                bancos_retornados += 1 # Considera como um "resultado" processado

        if bancos_retornados == 0 and "error" not in resposta_api_geral: # Se iterou e não achou nada conhecido
            st.info("Nenhum resultado de carga para exibir ou formato de resposta não reconhecido.")

def _preparar_ator_para_exibicao(ator_dict: dict) -> dict:
    """Prepara dados do ator para exibição, como URL de poster."""
    ator_copy = ator_dict.copy()
    # Assumindo que o _id do ator (que é o ator_id) é usado para a imagem
    id_para_poster = ator_copy.get("id", ator_copy.get("_id", ator_copy.get("ator_id", "")))
    ator_copy["poster_url"] = f"assets/imagens/actors/{id_para_poster}.jpg"
    
    nascimento = ator_copy.get("ano_nascimento")
    if nascimento and isinstance(nascimento, (int, float)):
        try:
            ator_copy["idade_aproximada"] = 2025 - int(nascimento) # Usando um ano base
        except ValueError:
            ator_copy["idade_aproximada"] = "N/A"
    else:
        ator_copy["idade_aproximada"] = "N/A"
    return ator_copy

def renderizar_detalhes_do_filme_completo(
    detalhes_filme_resposta: dict,
    atores_filme_resposta: dict,
    nome_banco_amigavel: str
):
    """Renderiza a página de detalhes do filme, incluindo informações do filme e dos atores."""

    st.subheader(f"🎬 Detalhes do Filme (Banco: {nome_banco_amigavel})")

    # --- Exibir Detalhes do Filme ---
    if "error" in detalhes_filme_resposta:
        st.error(f"Não foi possível carregar os detalhes do filme: {detalhes_filme_resposta['error']}")
        return

    # A resposta do backend para detalhes de um filme deve ser o próprio dicionário do filme.
    # Se estiver aninhado em 'data', ajuste aqui:
    filme_data = detalhes_filme_resposta.get("data", detalhes_filme_resposta)
    if not isinstance(filme_data, dict) or not filme_data:
         st.warning("Detalhes do filme não encontrados ou em formato inesperado.")
         return

    filme_display = _preparar_filme_para_exibicao(filme_data) # Sua função auxiliar que já deve existir

    col1, col2 = st.columns([1.5, 3.5]) # Ajuste a proporção conforme necessário
    with col1:
        if os.path.exists(filme_display.get("poster_url", "")):
            try:
                st.image(Image.open(filme_display["poster_url"]), width=300) # Aumentar um pouco
            except Exception as e:
                st.caption(f"Poster não disponível (erro ao carregar).")
                print(f"Erro ao carregar imagem {filme_display.get('poster_url', '')}: {e}")
        else:
            st.caption(f"Poster não disponível para {filme_display.get('titulo', 'filme')}")

    with col2:
        titulo_filme = filme_display.get('titulo', 'Título Desconhecido')
        ano = int(filme_display.get('ano_lancamento', 0)) if filme_display.get('ano_lancamento') else "N/A"
        st.title(f"{titulo_filme} ({ano})")

        nota_formatada = filme_display.get('nota_formatada', 'N/A')
        votos_formatados = filme_display.get('votos_formatados', 'N/A')

        if nota_formatada == '0.0':
            st.markdown("⭐ Ainda não lançado | 🗳️ Votos indisponíveis")
        else:
            st.markdown(f"⭐ **Nota:** {nota_formatada}  |  🗳️ **Votos:** {votos_formatados}")

        
        tipo_producao = filme_display.get('tipo', 'N/A')
        st.markdown(f"**Tipo:** {tipo_producao}")

        generos_lista = filme_display.get('generos', [])
        st.markdown(f"**Gêneros:** {', '.join(generos_lista) if generos_lista else 'N/A'}")

        if tipo_producao and tipo_producao.lower() != "jogo" and nota_formatada != '0.0':
            duracao_filme = int(filme_display.get('duracao', 0)) if filme_display.get('duracao') else 0
            st.markdown(f"**Duração:** {duracao_filme} min.")
        
        st.markdown(f"**ID ({nome_banco_amigavel}):** `{filme_display.get('_id', filme_display.get('id', 'N/A'))}`")


    sinopse = filme_display.get('sinopse', 'Sinopse não disponível.')
    st.subheader("📝 Sinopse")
    st.markdown(sinopse if sinopse else "_Não há sinopse disponível para este título._")
    st.markdown("---")

    # --- Exibir Atores do Filme ---
    st.subheader("🎭 Elenco Principal")
    
    # Adiciona uma mensagem antes de carregar as imagens do elenco
    if atores_filme_resposta and not ("error" in atores_filme_resposta and not atores_filme_resposta.get("data")) : # Se não for um erro direto
        placeholder_elenco = st.empty() # Cria um placeholder
        if isinstance(atores_filme_resposta, list) and not atores_filme_resposta : # Lista vazia
             placeholder_elenco.info("Nenhuma informação de elenco encontrada para este filme neste banco.")
        elif isinstance(atores_filme_resposta, dict) and not atores_filme_resposta.get("data") and not ("error" in atores_filme_resposta and atores_filme_resposta.get("error")): # Dict com data vazia
             placeholder_elenco.info("Nenhuma informação de elenco encontrada para este filme neste banco.")
        else:
            placeholder_elenco.caption("Preparando imagens do elenco...") # Mensagem enquanto as imagens abaixo são carregadas/processadas

    if "error" in atores_filme_resposta:
        st.error(f"Não foi possível carregar os atores do filme: {atores_filme_resposta['error']}")
    else:
        atores_lista_bruta = [] 
        if isinstance(atores_filme_resposta, list):
            atores_lista_bruta = atores_filme_resposta
        elif isinstance(atores_filme_resposta, dict):
            if "error" in atores_filme_resposta: # Erro específico para este bloco de atores
                st.error(f"Não foi possível carregar os atores do filme: {atores_filme_resposta['error']}")
            else:
                atores_lista_bruta = atores_filme_resposta.get("data", atores_filme_resposta.get("atores", []))
        
        if isinstance(atores_lista_bruta, list) and atores_lista_bruta:
            col_atores = st.columns(3)
            for i, ator_data in enumerate(atores_lista_bruta):
                with col_atores[i % 3]:
                    # ... (seu código para exibir cada ator, com Image.open, ImageOps.fit, etc.) ...
                    # A sugestão de usar o caption com quebra de linha já está aqui
                    ator_display = _preparar_ator_para_exibicao(ator_data)
                    nome_ator_str = ator_display.get('nome_ator', 'Nome Desconhecido')
                    nome_personagem_str = ator_display.get('nome_personagem', 'N/A')
                    # Monta o caption com quebra de linha (dois espaços antes do \n)
                    # Você pode usar markdown para negrito no nome do ator se quiser
                    #caption_texto = f"**{nome_ator_str}** \nComo: {nome_personagem_str}"
                    # Alternativa para quebra de linha se a anterior não funcionar perfeitamente em todos os casos:
                    caption_texto = f"**{nome_ator_str}** \n\n como {nome_personagem_str}" # Usando tag HTML <br>

                    # Define as dimensões padrão para a imagem
                    TARGET_ATOR_WIDTH = 200 
                    TARGET_ATOR_HEIGHT = 249 # Para o placeholder e ImageOps.fit


                    if os.path.exists(ator_display.get("poster_url", "")):
                        try:
                            img_ator = Image.open(ator_display["poster_url"])
                            img_ator_processada = ImageOps.fit(img_ator, (TARGET_ATOR_WIDTH, TARGET_ATOR_HEIGHT), Image.Resampling.LANCZOS)
                            st.image(img_ator_processada, caption=caption_texto, width=TARGET_ATOR_WIDTH, use_container_width='never')
                        # ... (seu except e else com placeholders)
                        except Exception as e_img:
                            st.markdown(f"""<div style='width:{TARGET_ATOR_WIDTH}px; height:{TARGET_ATOR_HEIGHT}px; background-color:#333; display:flex; align-items:center; justify-content:center; border-radius:8px; color:grey;font-size:small;text-align:center;'>Foto Indisponível</div>""", unsafe_allow_html=True)
                            st.caption(caption_texto)
                    else:
                        st.markdown(f"""<div style='width:{TARGET_ATOR_WIDTH}px; height:{TARGET_ATOR_HEIGHT}px; background-color:#333; display:flex; align-items:center; justify-content:center; border-radius:8px; color:grey;font-size:small;text-align:center;'>Foto Indisponível</div>""", unsafe_allow_html=True)
                        st.caption(caption_texto)
                    st.write("") 
            
            if 'placeholder_elenco' in locals(): # Limpa a mensagem "Preparando imagens..." após o loop
                placeholder_elenco.empty()

        elif isinstance(atores_lista_bruta, list) and not atores_lista_bruta and 'placeholder_elenco' not in locals() : # Se placeholder não foi usado antes
            st.info("Nenhuma informação de elenco encontrada para este filme neste banco.")
        elif not isinstance(atores_lista_bruta, list): # Se não for lista (e não erro já tratado)
            st.warning("Informações de elenco em formato inesperado.")

# src/streamlit_app/components/display_utils.py
# ...

def processar_e_exibir_resposta_escrita_api(
    resposta_api_completa: Dict[str, Any], 
    banco_selecionado_chave: str, # "mongo", "todos", etc.
    operacao_label: str # Ex: "Atualização de Filme"
):
    if not isinstance(resposta_api_completa, dict):
        st.error(f"Resposta da API para '{operacao_label}' em formato inesperado.")
        st.json(resposta_api_completa)
        return

    # Checa erro global da API primeiro
    if "error" in resposta_api_completa and "status" not in resposta_api_completa :
        st.error(f"Falha na {operacao_label.lower()}: {resposta_api_completa['error']}")
        return
    
    if resposta_api_completa.get("status") != "sucesso":
        st.warning(f"API não retornou status 'sucesso' para {operacao_label.lower()}. Mensagem: {resposta_api_completa.get('mensagem', 'N/A')}")
        # Mesmo assim, pode haver detalhes em 'dados' se for modo 'todos'

    # O endpoint, quando bem-sucedido, envelopa o resultado em "dados" via resposta_sucesso.
    dados_retornados_pela_api = resposta_api_completa.get("dados")

    if dados_retornados_pela_api is None and resposta_api_completa.get("status") == "sucesso":
        # Isso pode acontecer se a resposta_sucesso foi chamada sem 'dados' payload
        # Ex: para uma deleção bem-sucedida que só retorna mensagem.
        st.success(f"✅ {BANCOS_SUPORTADOS.get(banco_selecionado_chave, banco_selecionado_chave.capitalize())}: {resposta_api_completa.get('mensagem', operacao_label + ' realizada.')}")
        return

    if not isinstance(dados_retornados_pela_api, dict):
        st.error(f"Estrutura de 'dados' inesperada na resposta da API para {operacao_label.lower()}.")
        st.json(resposta_api_completa)
        return

    if banco_selecionado_chave != "todos":
        # Para banco único, dados_retornados_pela_api é o filme atualizado (se for AtualizarFilme)
        # ou o dict de mensagem (se for RemoverFilme, já tratado acima se 'dados' for None).
        # O endpoint de atualização para banco único agora retorna o FilmeResponse diretamente (não envelopado por resposta_sucesso).
        # E o de remoção retorna OperacaoStatusResponse.
        # Esta função `processar_e_exibir_resposta_escrita_api` precisa ser mais específica
        # ou a `processar_e_exibir_resposta_simples_api` ser adaptada.

        # Vamos reusar a lógica da `processar_e_exibir_resposta_simples_api` que você já tem.
        # Ela espera que `resposta_api` seja o dict que contém "data" (filme) ou "message".
        # Se o endpoint de banco único retorna o FilmeResponse diretamente,
        # e o de remoção retorna OperacaoStatusResponse, a `processar_e_exibir_resposta_simples_api`
        # precisaria ser chamada com `resposta_api_completa` diretamente.
        
        # Por agora, vamos focar no caso "todos". O caso de banco único
        # para escrita já deve estar sendo tratado pela `processar_e_exibir_resposta_simples_api`
        # se o endpoint retornar o formato que ela espera.
        # Se o endpoint de atualização para banco único retorna o FilmeResponse diretamente (não um dict com 'data'),
        # então a página do Streamlit que chama isso precisa tratar de forma diferente.
        # VOU ASSUMIR QUE O CASO DE BANCO ÚNICO JÁ ESTÁ SENDO TRATADO ADEQUADAMENTE PELA SUA UI.
        # O FOCO AQUI É O MODO "TODOS".
         nome_banco_amigavel = BANCOS_SUPORTADOS.get(banco_selecionado_chave, banco_selecionado_chave.capitalize())
         st.info(f"Resultado para {operacao_label} em {nome_banco_amigavel}:")
         # Se o endpoint de atualização para banco único agora retorna FilmeResponse direto,
         # e o de remoção OperacaoStatusResponse, `resposta_api_completa` JÁ É esse objeto.
         # O display_utils precisaria de funções para exibir esses objetos Pydantic.
         # Por ora, vamos exibir o JSON.
         st.json(resposta_api_completa)


    else: # Modo "todos"
        st.subheader(f"{operacao_label} (Status por Banco)")
        # dados_retornados_pela_api é o dict {"mongo": {"data": ..., "message": ...} ou {"error": ...}, ...}
        
        for nome_banco_int, res_banco in dados_retornados_pela_api.items():
            if nome_banco_int not in BANCOS_SUPORTADOS: continue

            nome_banco_amigavel_loop = BANCOS_SUPORTADOS.get(nome_banco_int, nome_banco_int.capitalize())
            with st.expander(f"Resultado para {nome_banco_amigavel_loop}:", expanded=True):
                if isinstance(res_banco, dict):
                    if "error" in res_banco:
                        st.error(f"❌ Falha: {res_banco['error']} (Status: {res_banco.get('status_code', 'N/A')})")
                    elif "data" in res_banco and res_banco["data"] is not None: # Para atualização, data é o filme
                        st.success(f"✅ {res_banco.get('message', operacao_label + ' bem-sucedida!')}")
                        st.caption("Dados retornados:")
                        st.json(res_banco["data"])
                    elif "message" in res_banco: # Para remoção bem-sucedida
                        st.success(f"✅ {res_banco['message']}")
                    else:
                        st.info("Resposta do banco sem detalhes claros.")
                        st.json(res_banco)
                else:
                    st.warning(f"Formato de resposta inesperado do {nome_banco_amigavel_loop}.")
                    st.json(res_banco)
        st.markdown("---")

# src/streamlit_app/components/display_utils.py
import streamlit as st
import pandas as pd
import plotly.express as px
from typing import Dict, Any, List, Union
from config.settings import BANCOS_SUPORTADOS

# Suas funções _plotar_... continuam aqui. Vou incluí-las para referência.

def _plotar_grafico_contagem_unico_banco(dados_contagem_lista: List[Dict[str, Any]], nome_banco_amigavel_plot: str):
    """Função auxiliar interna para plotar o gráfico de contagem para UM banco."""
    if not dados_contagem_lista:
        st.info(f"Nenhum dado de contagem retornado para {nome_banco_amigavel_plot}.")
        return
    try:
        df = pd.DataFrame(dados_contagem_lista)
        # O modelo Pydantic ContagemPorAnoResponse garante que a chave 'ano' exista no JSON final
        if 'ano' not in df.columns or 'quantidade' not in df.columns:
            col_ano = '_id' if '_id' in df.columns else 'ano' # Fallback para _id
            if col_ano not in df.columns:
                 st.error(f"Dados de contagem para {nome_banco_amigavel_plot} não contêm 'ano' ou '_id'. Colunas: {df.columns.tolist()}")
                 return
            df = df.rename(columns={col_ano: 'ano'})

        df['ano'] = pd.to_numeric(df['ano'], errors='coerce').dropna().astype(int)
        df = df.sort_values(by='ano')
        fig = px.line(df, x='ano', y='quantidade', title=f"Filmes por Ano - {nome_banco_amigavel_plot}", labels={"ano": "Ano", "quantidade": "Nº de Filmes"}, markers=True)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("---")
    except Exception as e:
        st.error(f"Erro ao plotar gráfico de contagem para {nome_banco_amigavel_plot}: {e}")
        st.json(dados_contagem_lista)

def _plotar_grafico_media_genero_unico_banco(dados_media_lista: List[Dict[str, Any]], nome_banco_amigavel_plot: str):
    """Função auxiliar interna para plotar o gráfico de média de notas por gênero para UM banco."""
    if not dados_media_lista:
        st.info(f"Nenhum dado de média de notas retornado para {nome_banco_amigavel_plot}.")
        return
    try:
        df = pd.DataFrame(dados_media_lista)
        if 'genero' not in df.columns or 'media_nota' not in df.columns:
            st.error(f"Dados de média para {nome_banco_amigavel_plot} não contêm 'genero' ou 'media_nota'. Colunas: {df.columns.tolist()}")
            return
        df['media_nota'] = pd.to_numeric(df['media_nota'], errors='coerce')
        df = df.dropna(subset=['media_nota']).sort_values(by='media_nota', ascending=False)
        orientation = 'h' if len(df) > 12 else 'v'
        df_plot = df.head(25) if orientation == 'v' else df.sort_values(by='media_nota', ascending=True).tail(25)
        fig = px.bar(df_plot, x='genero' if orientation == 'v' else 'media_nota', y='media_nota' if orientation == 'v' else 'genero',
                     orientation=orientation, title=f"Média de Notas por Gênero - {nome_banco_amigavel_plot}",
                     labels={"genero": "Gênero", "media_nota": "Média das Notas"},
                     color='media_nota', color_continuous_scale=px.colors.sequential.Tealgrn, text_auto='.2f')
        fig.update_traces(textposition='outside')
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("---")
    except Exception as e:
        st.error(f"Erro ao plotar gráfico de média para {nome_banco_amigavel_plot}: {e}")
        st.json(dados_media_lista)

# --- FUNÇÕES DE DISPLAY PRINCIPAIS (CORRIGIDAS) ---

# src/streamlit_app/components/display_utils.py
# ... (imports e as funções auxiliares _plotar_... que você já tem) ...

def exibir_grafico_contagem_por_ano(
    resposta_api_completa: Dict[str, Any], 
    banco_selecionado_chave: str 
):
    """Exibe o gráfico de contagem por ano, tratando respostas de banco único e de 'todos'."""
    #print(f"DEBUG DISPLAY (Contagem Ano): Banco Chave: '{banco_selecionado_chave}'")
    
    if not isinstance(resposta_api_completa, dict) or resposta_api_completa.get("status") != "sucesso":
        msg_erro = resposta_api_completa.get("error", resposta_api_completa.get("mensagem", "Resposta da API inválida ou sem sucesso."))
        st.error(f"Não foi possível obter dados para contagem: {msg_erro}")
        if isinstance(resposta_api_completa, dict): st.json(resposta_api_completa)
        return

    if banco_selecionado_chave != "todos":
        # Para banco único, o endpoint envelopa os dados na chave "contagem_por_ano".
        dados_para_plotar = resposta_api_completa.get("contagem_por_ano")
        if isinstance(dados_para_plotar, list):
            nome_banco_amigavel = BANCOS_SUPORTADOS.get(banco_selecionado_chave, banco_selecionado_chave.capitalize())
            with st.spinner(f"Gerando gráfico para {nome_banco_amigavel}..."):
                _plotar_grafico_contagem_unico_banco(dados_para_plotar, nome_banco_amigavel)
        else:
            st.warning(f"Dados de 'contagem_por_ano' não encontrados ou em formato inválido para o banco '{banco_selecionado_chave}'.")
            st.json(resposta_api_completa)
    
    else: # Modo "todos"
        algum_dado_plotado = False
        # Itera DIRETAMENTE sobre a resposta da API, pulando chaves que não são de bancos
        for nome_banco_json, resultado_banco_especifico in resposta_api_completa.items():
            if nome_banco_json not in BANCOS_SUPORTADOS or nome_banco_json == "todos": 
                continue # Pula chaves como "status", "mensagem", etc.

            nome_banco_amigavel_loop = BANCOS_SUPORTADOS.get(nome_banco_json, nome_banco_json.capitalize())
            
            with st.spinner(f"Gerando gráfico para {nome_banco_amigavel_loop}..."):
                if isinstance(resultado_banco_especifico, dict):
                    if "error" in resultado_banco_especifico:
                        st.error(f"Erro ao buscar contagem para {nome_banco_amigavel_loop}: {resultado_banco_especifico['error']}")
                    else:
                        dados_contagem_lista = resultado_banco_especifico.get("data")
                        if isinstance(dados_contagem_lista, list):
                            _plotar_grafico_contagem_unico_banco(dados_contagem_lista, nome_banco_amigavel_loop)
                            algum_dado_plotado = True
                        else:
                            st.info(f"Nenhum dado de contagem encontrado para {nome_banco_amigavel_loop}.")
                            st.markdown("---")
                else: 
                    st.warning(f"Formato de resposta inesperado para {nome_banco_amigavel_loop}."); st.json(resultado_banco_especifico)
        
        if not algum_dado_plotado:
            st.info("Nenhum dado de contagem disponível para plotagem no modo 'todos'.")

def exibir_grafico_media_notas_genero(
    resposta_api_completa: Dict[str, Any], 
    banco_selecionado_chave: str 
):
    """Exibe o gráfico de média de notas, tratando respostas de banco único e de 'todos'."""
    #print(f"DEBUG DISPLAY (Media Gênero): Banco Chave: '{banco_selecionado_chave}'")

    if not isinstance(resposta_api_completa, dict) or resposta_api_completa.get("status") != "sucesso":
        msg_erro = resposta_api_completa.get("error", resposta_api_completa.get("mensagem", "Resposta da API inválida ou sem sucesso."))
        st.error(f"Não foi possível obter dados para média de notas: {msg_erro}")
        if isinstance(resposta_api_completa, dict): st.json(resposta_api_completa)
        return

    if banco_selecionado_chave != "todos":
        # Para banco único, os dados estão na chave "media_notas_por_genero" no nível raiz
        dados_para_plotar = resposta_api_completa.get("media_notas_por_genero")
        if isinstance(dados_para_plotar, list):
            nome_banco_amigavel = BANCOS_SUPORTADOS.get(banco_selecionado_chave, banco_selecionado_chave.capitalize())
            with st.spinner(f"Gerando gráfico para {nome_banco_amigavel}..."):
                _plotar_grafico_media_genero_unico_banco(dados_para_plotar, nome_banco_amigavel)
        else:
            st.warning(f"Dados de 'media_notas_por_genero' não encontrados ou em formato inválido para o banco '{banco_selecionado_chave}'.")
            st.json(resposta_api_completa)
    
    else: # Modo "todos"
        algum_dado_plotado = False
        # Itera DIRETAMENTE sobre a resposta da API
        for nome_banco_json, resultado_banco_especifico in resposta_api_completa.items():
            if nome_banco_json not in BANCOS_SUPORTADOS or nome_banco_json == "todos":
                continue

            nome_banco_amigavel_loop = BANCOS_SUPORTADOS.get(nome_banco_json, nome_banco_json.capitalize())
            
            with st.spinner(f"Gerando gráfico para {nome_banco_amigavel_loop}..."):
                if isinstance(resultado_banco_especifico, dict):
                    if "error" in resultado_banco_especifico:
                        st.error(f"Erro ao buscar média para {nome_banco_amigavel_loop}: {resultado_banco_especifico['error']}")
                    else:
                        dados_media_lista = resultado_banco_especifico.get("data")
                        if isinstance(dados_media_lista, list):
                            _plotar_grafico_media_genero_unico_banco(dados_media_lista, nome_banco_amigavel_loop)
                            algum_dado_plotado = True
                        else:
                            st.info(f"Nenhum dado de média encontrado para {nome_banco_amigavel_loop}.")
                            st.markdown("---")
                else: 
                    st.warning(f"Formato inesperado para {nome_banco_amigavel_loop}."); st.json(resultado_banco_especifico)
        
        if not algum_dado_plotado:
            st.info("Nenhum dado de média de notas disponível para plotagem no modo 'todos'.")