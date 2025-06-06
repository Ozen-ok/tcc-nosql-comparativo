# tcc-nosql-comparativo
# 🎓 TCC: Análise Comparativa de Bancos de Dados NoSQL

## "Aplicabilidade e desempenho de banco de dados NoSQL em aplicações modernas: um estudo de caso em MongoDB, Cassandra, Neo4j e Redis."

Este repositório contém o código-fonte e os resultados do projeto de TCC que implementa e avalia o desempenho de quatro bancos de dados NoSQL populares em diferentes cenários de carga de trabalho. A aplicação consiste em uma API FastAPI que se comunica com os bancos e uma interface interativa com Streamlit para visualização e demonstração.

---

### 🎯 Objetivos do Projeto

* **Comparar** o desempenho de MongoDB, Cassandra, Neo4j e Redis em operações de escrita, leitura complexa e agregações.
* **Desenvolver** uma aplicação moderna e dockerizada utilizando Python, com uma API (FastAPI) e um dashboard interativo (Streamlit).
* **Demonstrar** na prática a aplicabilidade e os "trade-offs" de cada banco de dados, mostrando que a escolha da tecnologia depende do problema a ser resolvido.
* **Analisar** os resultados de performance para fornecer conclusões embasadas sobre a adequação de cada banco para diferentes cenários.

---

### 🛠️ Tecnologias Utilizadas

* **Backend:** Python, FastAPI
* **Frontend:** Streamlit
* **Bancos de Dados:**
    * 🍃 **MongoDB:** Banco de dados orientado a documentos.
    * 🗄️ **Cassandra:** Banco de dados colunar e distribuído.
    * 🕸️ **Neo4j:** Banco de dados de grafos.
    * ⚡ **Redis:** Banco de dados em memória (chave-valor).
* **Infraestrutura:** Docker, Docker Compose
* **Validação de Dados:** Pydantic

---

### 🗂️ Estrutura do Projeto

O projeto utiliza o padrão `src-layout` para uma organização de código limpa e manutenível.

```bash
tcc-nosql-comparativo/
├── .streamlit/
├── data/
│   ├── atores.tsv
│   ├── elenco.tsv
│   └── filmes.tsv
├── testes/
│   ├── teste_insercao.py
│   ├── teste_busca_avancada.py
│   └── ...
├── src/
│   ├── api/
│   ├── core/
│   ├── databases/
│   ├── models/
│   ├── services/
│   ├── streamlit_app/
│   └── utils/
├── .env
├── .gitignore
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

---

### 🚀 Como Executar o Projeto

Siga os passos abaixo para configurar e executar o ambiente completo.

#### 1. Pré-requisitos

* **Docker** e **Docker Compose** instalados.
* **Python 3.9+** instalado na sua máquina local.

#### 2. Configuração do Ambiente

**a. Clone o Repositório**
```bash
git clone [URL_DO_SEU_REPOSITORIO]
cd tcc-nosql-comparativo
```

**b. Crie o Arquivo de Ambiente**
Crie uma cópia do arquivo `.env.example` (se você criar um) ou crie um arquivo `.env` na raiz do projeto com as variáveis de ambiente para as conexões dos bancos. Exemplo:
```env
# MongoDB
MONGO_INITDB_ROOT_HOST=mongodb
MONGO_INITDB_ROOT_PORT=27017
# ... (outras variáveis para os 4 bancos)
```

**c. Configure o Ambiente Virtual Python**
É recomendado usar um ambiente virtual para instalar as dependências.
```bash
# Crie o ambiente (use .venv ou o nome que preferir)
python -m venv .venv

# Ative o ambiente
# No Windows:
# .\.venv\Scripts\activate
# No macOS/Linux:
# source .venv/bin/activate

# Instale as dependências
pip install -r requirements.txt
```

#### 3. Execução da Aplicação

**a. Inicie os Serviços com Docker Compose**
Este comando vai construir a imagem da sua aplicação e iniciar todos os contêineres (API e os 4 bancos de dados).
```bash
docker-compose up --build -d
```
O `-d` executa os contêineres em modo "detached" (em segundo plano).

**b. Acesse a API**
* A API FastAPI estará rodando em `http://localhost:8000`.
* A documentação interativa (Swagger UI) estará disponível em `http://localhost:8000/docs`.

**c. Execute o Frontend Streamlit**
Com o ambiente virtual ativado, rode o seguinte comando no terminal:
```bash
streamlit run src/streamlit_app/app.py
```
* A aplicação Streamlit estará disponível no seu navegador em `http://localhost:8501`.

#### 4. Execução dos Testes de Desempenho

Os scripts para os testes de performance estão na pasta `/testes`. Para executá-los, certifique-se de que os contêineres Docker estão no ar e execute os scripts pelo terminal (com o ambiente virtual ativado).

```bash
# Exemplo para o teste de inserção
python testes/teste_insercao.py

# Exemplo para o teste de busca
python testes/teste_busca_avancada.py
```
Os resultados serão salvos em arquivos `.json` dentro da mesma pasta.

---

### 👨‍💻 Autor

**Ozen** 

![alt text](https://raw.githubusercontent.com/Ozen-ok/tcc-nosql-comparativo/refs/heads/main/assets/plankton.png) "Plankton")
