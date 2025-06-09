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

---

### 🚀 Configuração e Execução

Siga os passos abaixo para configurar e executar o ambiente completo do projeto.

#### 1. Pré-requisitos
* [Docker](https://www.docker.com/products/docker-desktop/) instalado e em execução.
* [Docker Compose](https://docs.docker.com/compose/install/) (geralmente já vem com o Docker Desktop).

#### 2. Arquivo de Ambiente

Este projeto utiliza variáveis de ambiente para configurar as conexões.

Crie uma cópia do arquivo de exemplo .env.exemplo e renomeie-a para .env:

```bash
cp .env.exemplo .env
```

Abra o arquivo .env recém-criado e preencha as senhas e outras configurações conforme necessário.

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
#### 3. Execução da Aplicação
Com o Docker em execução e o arquivo .env criado, um único comando é necessário para iniciar toda a aplicação:

COMPOSE_BAKE=true docker-compose up --build

Usar o Bake (COMPOSE_BAKE=true) permite que você passe a definição de compilação completa para todos os serviços e orquestre a execução da compilação da maneira mais eficiente.
O comando docker-compose up irá construir a imagem da aplicação (se ainda não existir) e iniciar todos os contêineres definidos: a API, o Frontend e os 4 bancos de dados.
Use a flag -d (docker-compose up --build -d) se desejar executar os contêineres em segundo plano (modo "detached").

#### 4. Acessando os Serviços
Após a inicialização, os serviços estarão disponíveis nos seguintes endereços:

Frontend (Streamlit): http://localhost:8501
Backend (API FastAPI): http://localhost:8000
Documentação da API (Swagger): http://localhost:8000/docs
Mongo Express (Interface para MongoDB): http://localhost:8081

🧪 Execução dos Testes de Desempenho
Os scripts para os testes de performance estão na pasta /testes. Para executá-los, certifique-se de que os contêineres da aplicação estão no ar (docker-compose up) e, em um novo terminal, execute os scripts desejados.

Observação: Os scripts de teste se conectarão à API na porta 8000 para realizar as operações.

# Exemplo para o teste de inserção
python testes/teste_insercao.py

# Exemplo para o teste de busca
python testes/teste_busca_avancada.py

### 👨‍💻 Autor - **Ozen** 

![alt text](https://raw.githubusercontent.com/Ozen-ok/tcc-nosql-comparativo/refs/heads/main/assets/plankton.png)
