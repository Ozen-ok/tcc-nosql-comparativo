# src/streamlit_app/components/ui_elements.py
import streamlit as st
from typing import Dict, Optional, List # Adicione List se ainda não tiver
from config.settings import GENEROS_LISTA, ATORES_LISTA, CAMPOS_ORDENACAO_FILMES, BANCOS_SUPORTADOS, CAMPOS_ATUALIZAVEIS_FILME

# Chave única para armazenar a SELEÇÃO INTERNA do banco no session_state
CHAVE_SESSAO_BANCO_SELECIONADO_GLOBAL = "app_banco_selecionado_chave_interna"
# Chave para o widget selectbox em si (para o Streamlit controlar seu próprio estado)
CHAVE_WIDGET_SELECTBOX_BANCO = "selectbox_banco_global_na_sidebar"

def seletor_de_banco_global_sidebar() -> str:
    """
    Cria um selectbox na sidebar para o usuário escolher o banco de dados.
    A seleção é persistida entre as páginas usando st.session_state.
    Retorna a CHAVE INTERNA do banco selecionado (ex: "mongo", "todos").
    """
    # 1. Inicializa o estado global na primeira vez que o app roda ou se a chave não existir
    if CHAVE_SESSAO_BANCO_SELECIONADO_GLOBAL not in st.session_state:
        st.session_state[CHAVE_SESSAO_BANCO_SELECIONADO_GLOBAL] = "todos" # Ou seu default preferido

    # 2. Prepara as opções para o selectbox
    opcoes_labels = list(BANCOS_SUPORTADOS.values()) # Ex: ["MongoDB", "Cassandra", ..., "Todos os Bancos"]
    
    # 3. Determina o índice da seleção atual para o widget selectbox
    # Isso garante que o selectbox mostre o valor que está no nosso estado global.
    chave_interna_atual_no_estado = st.session_state[CHAVE_SESSAO_BANCO_SELECIONADO_GLOBAL]
    label_atual_no_estado = BANCOS_SUPORTADOS.get(chave_interna_atual_no_estado, BANCOS_SUPORTADOS["todos"]) # Fallback
    try:
        indice_selecao_atual = opcoes_labels.index(label_atual_no_estado)
    except ValueError: # Caso o valor no estado seja inválido (raro, mas por segurança)
        indice_selecao_atual = opcoes_labels.index(BANCOS_SUPORTADOS["todos"]) # Default para "Todos"
        st.session_state[CHAVE_SESSAO_BANCO_SELECIONADO_GLOBAL] = "todos" # Corrige o estado

    # 4. Criação do selectbox
    # O valor exibido/selecionado no widget é guardado pelo Streamlit na CHAVE_WIDGET_SELECTBOX_BANCO.
    # Quando o usuário muda, o callback on_change é chamado.
    st.sidebar.selectbox(
        "Banco de Dados Ativo:",  # Label do selectbox
        options=opcoes_labels,
        index=indice_selecao_atual,
        key=CHAVE_WIDGET_SELECTBOX_BANCO, # Chave para o widget em si
        on_change=atualizar_estado_banco_global # Função callback
    )

    # 5. A função retorna o valor ATUALIZADO da nossa chave de estado global
    return st.session_state[CHAVE_SESSAO_BANCO_SELECIONADO_GLOBAL]

def atualizar_estado_banco_global():
    """
    Callback para ser usado pelo on_change do selectbox.
    Atualiza a nossa chave de estado global (CHAVE_SESSAO_BANCO_SELECIONADO_GLOBAL)
    com base no valor selecionado no widget (que está em st.session_state[CHAVE_WIDGET_SELECTBOX_BANCO]).
    """
    label_selecionado_no_widget = st.session_state[CHAVE_WIDGET_SELECTBOX_BANCO]
    
    # Converte o label de volta para a chave interna
    for chave_interna, label_opcao in BANCOS_SUPORTADOS.items():
        if label_opcao == label_selecionado_no_widget:
            st.session_state[CHAVE_SESSAO_BANCO_SELECIONADO_GLOBAL] = chave_interna
            return
    
    # Fallback se algo der errado na conversão (não deveria acontecer se as opções são de BANCOS_SUPORTADOS)
    st.session_state[CHAVE_SESSAO_BANCO_SELECIONADO_GLOBAL] = "todos"
    print(f"WARN: Callback do seletor de banco não encontrou chave interna para o label: {label_selecionado_no_widget}. Usando 'todos'.")

def formulario_busca_avancada(chave_form: str = "form_busca_avancada"):
    """Renderiza o formulário de busca avançada e retorna os filtros."""
    filtros = {}
    with st.form(key=chave_form):
        st.markdown("#### Filtros de Busca")
        col1, col2 = st.columns(2)
        with col1:
            filtros["titulo"] = st.text_input("Título (parcial ou completo)")
            filtros["ano_lancamento_min"] = st.number_input("Ano de Lançamento (Mínimo)", min_value=1900, max_value=2025, value=1950)
            filtros["nota_min"] = st.number_input("Nota Mínima", min_value=0.0, max_value=10.0, value=5.0, step=0.1)
            filtros["duracao_min"] = st.number_input("Duração Mínima (minutos)", min_value=0, value=60)

        with col2:
            filtros["generos"] = st.multiselect("Gêneros", GENEROS_LISTA, placeholder="Selecione um ou mais gêneros...")
            filtros["tipo"] = st.selectbox("Tipo de Produção", ["Filme", "Série de TV", "Filme para TV", "Vídeo", "Jogo", "Minissérie", "Curta"], index=0)
            filtros["ordenar_por"] = st.selectbox("Ordenar Por", list(CAMPOS_ORDENACAO_FILMES.keys()))
            filtros["ordem"] = 1 if st.radio("Ordem", ["Crescente", "Decrescente"], index=1, horizontal=True) == "Crescente" else -1

        submitted = st.form_submit_button("🔎 Aplicar Filtros e Buscar")
        if submitted:
            # Converte nome amigável do campo de ordenação para o nome real
            filtros["ordenar_por"] = CAMPOS_ORDENACAO_FILMES[filtros["ordenar_por"]]
            if filtros["tipo"] == "Qualquer": filtros["tipo"] = None # Para não enviar o filtro se for qualquer um
            return filtros
    return None

def formulario_carregar_base(chave_form: str = "form_carregar_base"):
    """Renderiza o formulário para especificar os caminhos dos arquivos de dados."""
    with st.form(key=chave_form):
        st.subheader("📥 Defina os Caminhos para os Arquivos de Dados (.tsv)")
        # Usa os caminhos padrão como definido na estrutura do projeto
        filmes_path = st.text_input("Caminho do arquivo de filmes:", "data/filmes.tsv", help="Ex: data/filmes.tsv")
        atores_path = st.text_input("Caminho do arquivo de atores:", "data/atores.tsv", help="Ex: data/atores.tsv")
        elenco_path = st.text_input("Caminho do arquivo de elenco:", "data/elenco.tsv", help="Ex: data/elenco.tsv")
        
        submitted = st.form_submit_button("🚀 Iniciar Carga da Base de Dados")
        
        if submitted:
            if not all([filmes_path, atores_path, elenco_path]):
                st.error("Por favor, preencha todos os caminhos dos arquivos.")
                return None
            return {
                "filmes_path": filmes_path,
                "atores_path": atores_path,
                "elenco_path": elenco_path
            }
    return None

def formulario_atualizar_filme(chave_form: str = "form_atualizar_filme"):
    with st.form(key=chave_form):
        st.subheader("✏️ Atualizar Filme")
        id_filme = st.text_input("ID do Filme a ser atualizado (_id ou titulo_id):", placeholder="ex: tt0000001")
        
        campo_legivel = st.selectbox("Campo para Atualizar:", list(CAMPOS_ATUALIZAVEIS_FILME.keys()))
        campo_para_atualizar = CAMPOS_ATUALIZAVEIS_FILME[campo_legivel]
        
        novo_valor_str = None # Inicializa
        if campo_para_atualizar == "generos":
            novo_valor_str = st.multiselect(f"Novo(s) {campo_legivel}:", GENEROS_LISTA, help="Selecione os novos gêneros. A lista antiga será substituída.")
        elif campo_para_atualizar in ["nota"]:
            novo_valor_str = st.number_input(f"Novo Valor para {campo_legivel}:", value=7.0, step=0.1, format="%.1f")
        elif campo_para_atualizar in ["ano_lancamento", "numero_votos", "duracao"]:
            novo_valor_str = st.number_input(f"Novo Valor para {campo_legivel}:", value=0, step=1)
        else: # Para titulo, tipo, sinopse
            novo_valor_str = st.text_area(f"Novo Valor para {campo_legivel}:") if campo_para_atualizar == "sinopse" else st.text_input(f"Novo Valor para {campo_legivel}:")

        submitted = st.form_submit_button("🔄 Atualizar Campo do Filme")
        if submitted:
            if not id_filme or not campo_para_atualizar or novo_valor_str is None: # Gêneros pode ser lista vazia
                st.error("ID do Filme, Campo e Novo Valor são obrigatórios.")
                return None
            return {
                "id_filme": id_filme,
                "campo_para_atualizar": campo_para_atualizar,
                "novo_valor": novo_valor_str
            }
    return None

def formulario_deletar_filme(chave_form: str = "form_deletar_filme"):
    with st.form(key=chave_form):
        st.subheader("🗑️ Remover Filme")
        id_filme = st.text_input("ID do Filme a ser removido (_id ou titulo_id):", placeholder="ex: tt0000001")
        confirmacao = st.checkbox(f"Confirmo que desejo remover o filme com ID: {id_filme}", value=False)
        submitted = st.form_submit_button("❌ Remover Filme Permanentemente")
        if submitted:
            if not id_filme:
                st.error("ID do Filme é obrigatório.")
                return None
            if not confirmacao:
                st.warning("Você precisa confirmar a remoção.")
                return None
            return {"id_filme": id_filme}
    return None

def seletor_ator_para_filmes(chave_selectbox: str = "sb_ator_filmes_completo") -> Optional[Dict[str, str]]:
    """
    Cria um selectbox para o usuário escolher um ator.
    Retorna um dicionário {'id': str, 'nome': str} do ator selecionado,
    ou None se a opção placeholder "Selecione um ator..." for escolhida.
    """
    # ATORES_LISTA é sua lista de dicionários: [{'id': 'nm...', 'nome': 'Nome Ator'}, ...]
    
    # Adiciona uma opção inicial para o placeholder
    opcoes_display = ATORES_LISTA
    
    ator_objeto_selecionado = st.selectbox(
        label="Selecione o Ator:",
        options=opcoes_display,
        format_func=lambda ator_obj: ator_obj['nome'], # Mostra apenas o nome do ator para o usuário
        key=chave_selectbox # Chave única para este widget selectbox
    )
    
    # Se o usuário selecionou um ator real (não o placeholder)
    if isinstance(ator_objeto_selecionado, dict) and ator_objeto_selecionado.get("id") is not None:
        return ator_objeto_selecionado # Retorna o dicionário completo {'id': ..., 'nome': ...}
    
    return None # Retorna None se "Selecione um ator..." estiver selecionado

def renderizar_input_para_campo_atualizacao(
    campo_interno: str, 
    chave_valor_session_state: str, # Chave ÚNICA para o valor deste input no session_state
    valor_atual_do_banco = None # Opcional: para preencher com o valor existente do filme
):
    """Renderiza o widget de input apropriado, usando session_state para o valor."""
    
    label_amigavel = campo_interno.capitalize()
    for k_legivel, v_interno in CAMPOS_ATUALIZAVEIS_FILME.items():
        if v_interno == campo_interno:
            label_amigavel = k_legivel
            break
    
    # Se um valor atual do banco foi fornecido e o session_state para este input ainda não foi setado
    # pelo usuário, usamos o valor do banco como default inicial.
    # Se o usuário já interagiu, o valor do session_state tem precedência.
    if chave_valor_session_state not in st.session_state and valor_atual_do_banco is not None:
        st.session_state[chave_valor_session_state] = valor_atual_do_banco

    valor_para_widget = st.session_state.get(chave_valor_session_state)

    if campo_interno == "generos":
        default_generos = valor_para_widget if isinstance(valor_para_widget, list) else []
        # O widget multiselect já retorna o valor e o st.session_state é atualizado pelo 'key'
        st.multiselect(
            f"Novo(s) {label_amigavel}:", 
            GENEROS_LISTA, 
            default=default_generos,
            key=chave_valor_session_state # Usa a chave direto no widget
        )
    elif campo_interno == "nota":
        default_nota = float(valor_para_widget if valor_para_widget is not None else 0.0) # Default se None
        st.number_input(
            f"Novo Valor para {label_amigavel}:", 
            value=default_nota, 
            min_value=0.0, max_value=10.0, step=0.1, format="%.1f",
            key=chave_valor_session_state
        )
    elif campo_interno in ["ano_lancamento", "numero_votos", "duracao"]:
        default_num = int(valor_para_widget if valor_para_widget is not None else 0) # Default se None
        st.number_input(
            f"Novo Valor para {label_amigavel}:", 
            value=default_num, step=1,
            key=chave_valor_session_state
        )
    elif campo_interno == "tipo":
        tipos_producao = ["Filme", "Série de TV", "Filme para TV", "Vídeo", "Jogo", "Minissérie", "Curta"]
        # Se valor_para_widget for None ou não estiver na lista, default_index será 0
        try:
            default_index_tipo = tipos_producao.index(valor_para_widget) if valor_para_widget in tipos_producao else 0
        except ValueError: # Caso valor_para_widget não seja válido para index
             default_index_tipo = 0
        st.selectbox(
            f"Novo Valor para {label_amigavel}:", 
            tipos_producao, index=default_index_tipo,
            key=chave_valor_session_state
        )
    else: # Para titulo, sinopse
        default_text = str(valor_para_widget if valor_para_widget is not None else "")
        if campo_interno == "sinopse":
            st.text_area(
                f"Novo Valor para {label_amigavel}:", 
                value=default_text,
                key=chave_valor_session_state,
                height=120
            )
        else:
            st.text_input(
                f"Novo Valor para {label_amigavel}:", 
                value=default_text,
                key=chave_valor_session_state
            )
    # O valor do widget é automaticamente gerenciado pelo st.session_state devido ao uso de 'key'
    # Não precisamos retornar o valor aqui, a página vai ler do session_state.
    return st.session_state.get(chave_valor_session_state) # Retorna o valor atual do session_state


# src/streamlit_app/components/ui_elements.py
# ... (continuação do arquivo)

def formulario_interativo_atualizar_filme(prefixo_chave: str = "update_filme_int"): # Novo prefixo
    """
    Cria os inputs para atualizar filme de forma interativa usando on_change.
    Retorna um dicionário com id_filme, campo_para_atualizar e novo_valor 
    APENAS quando o botão de ação 'Atualizar Campo Agora' é clicado e os dados são válidos.
    Retorna None em outros casos (reruns normais, etc.).
    """
    st.subheader("✏️ Escolha o Filme e o Campo para Atualizar")

    # Chaves para o session_state (garantindo que sejam únicas para esta instância do formulário)
    id_filme_key_ss = f"{prefixo_chave}_id_filme_val"
    campo_legivel_key_ss = f"{prefixo_chave}_campo_legivel_val"
    # A chave para o novo_valor será dinâmica: f"{prefixo_chave}_novo_valor_dinamico_{campo_interno_selecionado}"

    # Inicializa valores no session_state se não existirem para esta instância do formulário
    if id_filme_key_ss not in st.session_state:
        st.session_state[id_filme_key_ss] = "tt10000000" # Default
    if campo_legivel_key_ss not in st.session_state:
        st.session_state[campo_legivel_key_ss] = list(CAMPOS_ATUALIZAVEIS_FILME.keys())[0] # Default para o primeiro campo

    # Callback para o on_change do selectbox de campo
    def ao_mudar_campo_para_atualizar_callback():
        # O selectbox com 'key' já atualizou st.session_state[campo_legivel_key_ss].
        # Opcional: limpar o valor do input anterior se os tipos forem muito diferentes.
        # Exemplo: se mudou de 'nota' (numérico) para 'título' (texto), o valor antigo pode não fazer sentido.
        # Esta parte é um pouco mais complexa porque a chave do valor é dinâmica.
        # Uma forma simples é não limpar, e renderizar_input_para_campo_atualizacao
        # tentará usar o valor do session_state se existir e for compatível.
        # Se você quiser limpar, precisaria saber qual era o campo *anterior*.
        # print(f"Campo selecionado mudou para: {st.session_state[campo_legivel_key_ss]}")
        pass # O rerun já vai redesenhar o input correto.

    # --- Widgets ---
    # ID do Filme (o valor é controlado pelo session_state via 'key')
    st.text_input(
        "ID do Filme a ser atualizado (_id):",
        key=id_filme_key_ss, # O widget lê e escreve em st.session_state[id_filme_key_ss]
    )

    # Campo a ser atualizado (o valor é controlado pelo session_state via 'key')
    st.selectbox(
        "Campo para Atualizar:",
        options=list(CAMPOS_ATUALIZAVEIS_FILME.keys()),
        key=campo_legivel_key_ss, # O widget lê e escreve em st.session_state[campo_legivel_key_ss]
        on_change=ao_mudar_campo_para_atualizar_callback
    )

    # Obtém o nome interno do campo com base na seleção atual do session_state
    campo_interno_selecionado = CAMPOS_ATUALIZAVEIS_FILME[st.session_state[campo_legivel_key_ss]]
    st.caption(f"Campo selecionado: **{st.session_state[campo_legivel_key_ss]}**. Insira o novo valor abaixo.")

    # Chave dinâmica para o session_state do input "novo valor"
    chave_novo_valor_ss_dinamica = f"{prefixo_chave}_novo_valor_dinamico_{campo_interno_selecionado}"
    
    # Renderiza o input para o novo valor.
    # A função renderizar_input... usa a chave_novo_valor_ss_dinamica para seu próprio widget e session_state.
    renderizar_input_para_campo_atualizacao(
        campo_interno_selecionado,
        chave_novo_valor_ss_dinamica
        # Se você tivesse um mecanismo para buscar o valor atual do filme para este campo,
        # passaria como valor_atual_do_banco.
    )

    # Botão de ação (NÃO é um st.form_submit_button)
    if st.button("🔄 Atualizar Filme", key=f"{prefixo_chave}_btn_acao_final"):
        id_filme_final = st.session_state.get(id_filme_key_ss)
        novo_valor_final = st.session_state.get(chave_novo_valor_ss_dinamica) # Pega o valor do session_state

        # Validação
        if not id_filme_final or not campo_interno_selecionado:
            st.error("ID do Filme e Campo para Atualizar são obrigatórios.")
            return None # Não retorna payload, a página principal não faz nada

        # Validação específica para o valor, dependendo do campo
        # (Pode ser mais elaborada, mas uma checagem básica)
        if campo_interno_selecionado == "generos":
            if not isinstance(novo_valor_final, list):
                st.warning("Gêneros devem ser uma lista (pode ser vazia para remover todos).")
                # Poderia tentar converter ou dar erro mais forte. Por ora, avisa.
        elif novo_valor_final is None or (isinstance(novo_valor_final, str) and not novo_valor_final.strip()):
             # Se for string e só espaços em branco, considera inválido (exceto para sinopse talvez)
             if campo_interno_selecionado != "sinopse" or novo_valor_final is None: # Sinopse pode ser string vazia
                 st.error("Novo Valor é obrigatório e não pode ser apenas espaços em branco (exceto para Sinopse que pode ser vazia).")
                 return None
        
        st.success(f"Disparando atualização! ID: {id_filme_final}, Campo: {campo_interno_selecionado}, Novo Valor: {novo_valor_final}")
        return {
            "id_filme": id_filme_final,
            "campo_para_atualizar": campo_interno_selecionado,
            "novo_valor": novo_valor_final # Este é o valor pego do session_state
        }
    
    return None # Retorna None se o botão não foi clicado

def formulario_deletar_filme(chave_form: str = "form_deletar_filme"):
    """Renderiza o formulário para deletar um filme."""
    with st.form(key=chave_form):
        st.subheader("🗑️ Remover Filme do Banco de Dados")
        # Usaremos o _id do filme (que agora é o titulo_id)
        id_filme = st.text_input("ID do Filme a ser removido (_id):", value="tt10000000", placeholder="ex: tt0000001")
        confirmacao = st.checkbox(f"Eu confirmo que desejo remover permanentemente o filme com ID: {id_filme}", value=False)
        
        submitted = st.form_submit_button("❌ Remover Filme")
        
        if submitted:
            if not id_filme:
                st.error("O ID do Filme é obrigatório para a remoção.")
                return None
            if not confirmacao:
                st.warning("Você precisa marcar a caixa de confirmação para remover o filme.")
                return None
            return {"id_filme": id_filme}
    return None

def formulario_inserir_filme(chave_form: str = "form_inserir_filme_novo"): # Chave um pouco diferente para evitar conflito
    """Renderiza o formulário para inserir um novo filme e retorna os dados como um dicionário."""

    # Valores padrão para facilitar o teste e o preenchimento
    # Usamos um contador no session_state para gerar IDs de título únicos para teste
    if 'filme_insert_count' not in st.session_state:
        st.session_state.filme_insert_count = 0

    # Gera um ID de título padrão para o placeholder. O usuário pode alterar.
    # O ideal é que o seu backend/CRUD do Mongo use titulo_id como _id para garantir unicidade.
    #default_titulo_id = f"tt{10000000 + st.session_state.filme_insert_count}"

    with st.form(key=chave_form):
        st.subheader("🎞️ Preencha os Dados do Novo Filme")

        # Usar colunas para melhor organização do formulário
        col1, col2 = st.columns(2)

        with col1:
            # Campo titulo_id é crucial, pois será usado como _id no MongoDB
            titulo_id = st.text_input("ID do Título (ex: tt1234567 - será o _id no MongoDB)", 
                                      value="tt10000000", 
                                      help="Este ID deve ser único.")
            titulo = st.text_input("Título do Filme", value="Titulo exemplo", placeholder="Insira o título do filme aqui...",)
            tipo_opcoes = ["Filme", "Série de TV", "Filme para TV", "Vídeo", "Jogo", "Minissérie", "Curta"]
            tipo = st.selectbox("Tipo de Produção", tipo_opcoes, index=0) # Default "Filme"
            ano_lancamento = st.number_input("Ano de Lançamento", min_value=1888, max_value=2030, value=2025)
            duracao = st.number_input("Duração (em minutos)", min_value=1, value=90, step=10)

        with col2:
            # GENEROS_LISTA deve estar definida em config.settings.py
            generos = st.multiselect("Gêneros", GENEROS_LISTA, default=["Ação", "Ficção Científica"])
            nota = st.number_input("Nota (0.0 a 10.0)", min_value=0.0, max_value=10.0, value=7.5, step=0.1, format="%.1f")
            numero_votos = st.number_input("Número de Votos", min_value=0, value=1000, step=100)
            sinopse = st.text_area("Sinopse", value="Sinopse exemplo", placeholder="Insira a sinopse aqui...", height=160)

        submitted = st.form_submit_button("➕ Adicionar Filme ao Banco de Dados")

        if submitted:
            # Validação básica no frontend (o backend também validará com Pydantic)
            if not all([titulo_id, titulo, tipo, ano_lancamento, generos, duracao]):
                st.error("Campos obrigatórios: ID do Título, Título, Tipo, Ano, Gêneros e Duração devem ser preenchidos.")
                return None

            # Incrementa o contador para o próximo ID de teste padrão
            st.session_state.filme_insert_count += 1

            # Retorna os dados do filme como um dicionário
            return {
                "titulo_id": titulo_id, # Este será usado como _id no MongoDB
                "titulo": titulo,
                "tipo": tipo,
                "ano_lancamento": ano_lancamento,
                "generos": generos,
                "nota": nota,
                "numero_votos": numero_votos,
                "duracao": duracao,
                "sinopse": sinopse
            }
    return None # Retorna None se o formulário não foi submetido