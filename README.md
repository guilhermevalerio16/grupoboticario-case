# Case para o Grupo Boticário

Neste arquivo, serão mostrados os detalhes do desenvolvimento para o case referente à vaga de Engenheiro de Dados para o Grupo Boticário.

## Case 2 - Carregamentos dos dados de vendas e da API do Spotify

Existem duas etapas para o desenvolvimento deste case:
- criação de 4 tabelas referentes aos arquivos de vendas;
- criação de 3 tabelas referentes aos dados da API do Spotify.

### Tasks desenvolvidas

Foi criado um projeto privado no Trello para criação das tasks contendo entregáveis
![image](https://github.com/guilhermevalerio16/grupoboticario-case/assets/61855053/f6d1c8e5-f9dc-494e-8e87-8dfa46a1fad6)

### Serviços utilizados:
- Visual Studio Code para desenvolvimento dos códigos e testes;
- Cloud Storage para armazenamento dos arquivos xlsx e dados da API do Spotify;
- BigQuery como data warehouse para disponibilização das tabelas demandadas no case;
- Cloud Composer para orquestração das pipelines;
- Cloud Build para criação do CI/CD responsável por fazer replace dos arquivos atualizados no GitHub dentro do bucket de DAGs no GCS

### Configuração do Ambiente

Para a execução deste desenvolvimento, é necessário ter uma conta de serviço no Google Cloud Platform com Billing ativo.

Siga os seguintes passos:

* Crie um projeto chamado `grupoboticario-case`;

* Crie um Bucket com nome `grupoboticario-case-datalake-sales` e insira os arquivos Excel de vendas:
    - `Base 2017.xlsx`
    - `Base_2018 (1).xlsx`
    - `Base_2019 (2).xlsx`;

* No BigQuery, no projeto `grupoboticario-case`:
    - crie um dataset chamado `refined_sales`;
    - crie um dataset chamado `spotify`;

* Ative a API do Composer, crie um novo ambiente e instale a dependência `openpyxl`;
* Crie uma chave JSON para esta conta e armazene-a no bucket `/home/airflow/gcs/data/`;

* No webserver do Airflow:
    * Em `Admin - Connections`, crie a seguinte conexão:
        - Connection Id: `GCP`;
        - Connection Type: `google_cloud_platform`;
        - Keyfile Path: adicione o caminho para a chave JSON criada.
    * Faça a adição das seguintes variáveis em `Admin - Variables`
        - Chave: `API_CLIENT_ID`; valor: seu `CLIENT_ID` gerado no Spotify;
        - Chave: `API_CLIENT_SECRET`; valor: seu `CLIENT_SECRET` gerado no Spotify;
        - Chave: `ROOT_PATH`; valor: `/home/airflow/gcs/dags/`

* No Cloud Build:
    - Em `triggers`, crie uma conexão com seu repositório no Github;
    - No setor `Advanced`, no subsetor `Substitution variables`, crie uma variável chamada `_GCS_BUCKET` e no valor adicione o bucket do GCS referente ao seu Composer

* Faça um push no seu repositório, aguarde os arquivos serem carregados no GCS e execute as DAGs:
  - `gb_sales_data_ingestion`
  - `gb_spotify_data_ingestion`

### BigQuery
![image](https://github.com/guilhermevalerio16/grupoboticario-case/assets/61855053/6c820d25-37bf-42c3-ac73-ad1f88711c6c)
### DAG de vendas
![image](https://github.com/guilhermevalerio16/grupoboticario-case/assets/61855053/554d5184-3219-4251-bb17-893a3df5632d)

![image](https://github.com/guilhermevalerio16/grupoboticario-case/assets/61855053/aff3591a-65f7-46eb-83cb-7ef5b7ad8988)


Autor: [Guilherme Valério](https://www.linkedin.com/in/guilhermevalerio/)
