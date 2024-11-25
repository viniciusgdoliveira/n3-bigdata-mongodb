<!-- @format -->

# Cannabis Strains Data Pipeline

Este projeto visa capturar, limpar, salvar e armazenar dados sobre diferentes cepas de cannabis em várias fontes: um arquivo CSV, um arquivo Parquet e um banco de dados MongoDB. Ele inclui funcionalidades de validação e limpeza de dados, bem como consultas no MongoDB para recuperar informações relevantes.

## Funcionalidades

1. **Captura de Dados (CSV)**: Carrega dados de um arquivo CSV para um DataFrame do Pandas.
2. **Validação e Limpeza de Dados**: Limpa os dados convertendo listas de strings em colunas específicas, removendo espaços extras e preenchendo valores ausentes.
3. **Salvamento em Formato Parquet**: Salva os dados limpos em um arquivo Parquet para otimizar o armazenamento e a leitura.
4. **Ingestão para MongoDB**: Insere os dados no banco de dados MongoDB, armazenando-os em uma coleção específica.
5. **Consultas no MongoDB**: Executa consultas no MongoDB para recuperar informações como as cepas mais bem avaliadas.

## Pré-requisitos

Antes de executar o projeto, certifique-se de que você tem os seguintes pré-requisitos instalados:

- **Python 3.x**
- **MongoDB**: Certifique-se de que o MongoDB esteja em execução no endereço `localhost:27017`.
- **Bibliotecas Python**: Instale as dependências com o comando:
  ```bash
  pip install pandas pymongo
  ```

## Estrutura do Projeto

```
Cannabis Strains Data Pipeline/
│
├── cannabis_strains.csv          # Arquivo CSV contendo dados das cepas
├── cannabis_strains.parquet      # Arquivo de saída em formato Parquet
├── main.py                       # Código principal do projeto
├── requirements.txt              # Arquivo usado para importar as bibliotecas
└── README.md                     # Este arquivo

```

## Como Usar

1. **Preparar o Banco de Dados MongoDB**:
   Certifique-se de que o MongoDB esteja configurado e em execução. O projeto usa o banco de dados `CannabisDB` e a coleção `Strains`. Assegure-se de que o MongoDB está acessível no `localhost:27017`.

2. **Arquivos CSV e Parquet**:

   - O arquivo CSV `cannabis_strains.csv` deve estar no mesmo diretório que o script.
   - O arquivo Parquet `cannabis_strains.parquet` será gerado automaticamente.

3. **Executando o Script**:
   Para executar o script, basta rodar o seguinte comando no terminal:

   ```bash
   python main.py
   ```

   O script realizará as seguintes etapas:

   - Carregar dados do arquivo CSV
   - Limpar e validar os dados
   - Salvar os dados em formato Parquet
   - Inserir os dados no banco de dados MongoDB
   - Executar uma consulta no MongoDB para exibir as 5 cepas mais bem avaliadas

## Consultas MongoDB

O script realiza uma consulta para listar as 5 cepas mais bem avaliadas, mostrando as seguintes informações:

- **Strain**: Nome da cepa
- **Type**: Tipo da cepa
- **Rating**: Avaliação
- **Effects**: Efeitos da cepa
- **Flavor**: Sabores
- **Description**: Descrição (primeiros 250 caracteres)

## Licença

Este projeto está licenciado sob a [MIT License](LICENSE).
