import pandas as pd
from pymongo import MongoClient

# MongoDB Configuração
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "CannabisDB"
COLLECTION_NAME = "Strains"

# Caminhos dos arquivos
CSV_FILE = "cannabis_strains.csv"
PARQUET_FILE = "cannabis_strains.parquet"

# 1. Pegar dados do CSV
def capture_data(file_path):
    print("Carregando dados do CSV...")
    try:
        data = pd.read_csv(file_path)
        print("Dados carregados com sucesso!")
        return data
    except FileNotFoundError:
        print("Erro: O arquivo CSV não foi encontrado.")
        return None
    except Exception as e:
        print(f"Erro ao carregar os dados: {e}")
        return None

# 2. Validar e limpar os dados
def clean_data(data):
    print("Iniciando limpeza de dados...")
    try:
        # Converter effects para listas
        data['Effects'] = data['Effects'].apply(lambda x: x.split(",") if pd.notnull(x) else [])
        # Converter flavor para listas
        data['Flavor'] = data['Flavor'].apply(lambda x: x.split(",") if pd.notnull(x) else [])
        # Remover espaços em branco extras
        data['Effects'] = data['Effects'].apply(lambda effects: [effect.strip() for effect in effects])
        data['Flavor'] = data['Flavor'].apply(lambda flavors: [flavor.strip() for flavor in flavors])
        # Preencher valores nulos na coluna 'Description'
        data['Description'] = data['Description'].fillna("")
        print("Dados limpos com sucesso!")
        return data
    except Exception as e:
        print(f"Erro na limpeza dos dados: {e}")
        return None

# 3. Salvar em Parquet
def save_to_parquet(data, file_path):
    print("Salvando dados no formato Parquet...")
    try:
        data.to_parquet(file_path, index=False)
        print("Dados salvos no arquivo Parquet!")
    except Exception as e:
        print(f"Erro ao salvar dados no Parquet: {e}")

# 4. Inserir no MongoDB
def ingest_to_mongodb(data):
    print("Conectando ao MongoDB...")
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        # Inserir dados no MongoDB
        print("Inserindo dados no MongoDB...")
        collection.insert_many(data.to_dict(orient="records"))
        print("Dados inseridos com sucesso!")
        client.close()
    except Exception as e:
        print(f"Erro ao inserir dados no MongoDB: {e}")

# 5. Query MongoDB
def query_mongodb():
    print("\nExecutando consultas no MongoDB...\n")
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        # Exempo de consulta: Top 5 cepas mais bem avaliadas
        print("Top 5 cepas mais bem avaliadas:\n")
        top_rated = collection.find().sort("Rating", -1).limit(5)
        
        # Print de cada cepa
        for strain in top_rated:
            print("-" * 60)  
            print(f"Cepa: {strain['Strain']}")
            print(f"Tipo: {strain['Type']}")
            print(f"Avaliação: {strain['Rating']}")
            print(f"Efeitos: {', '.join(strain['Effects']) if strain['Effects'] else 'No effects listed'}")
            print(f"Sabor: {', '.join(strain['Flavor']) if strain['Flavor'] else 'No flavors listed'}")
            print(f"Descrição: {strain['Description'][:250]}...")  
            print("-" * 60)  
        client.close()
    except Exception as e:
        print(f"Erro ao executar consultas no MongoDB: {e}")

# Função principal
def main():
    # Carregar dados
    data = capture_data(CSV_FILE)
    if data is None:
        return

    # Limpar dados
    cleaned_data = clean_data(data)
    if cleaned_data is None:
        return

    # Salvar em Parquet
    save_to_parquet(cleaned_data, PARQUET_FILE)

    # Inserir no MongoDB
    ingest_to_mongodb(cleaned_data)

    # Consultar MongoDB
    query_mongodb()

if __name__ == "__main__":
    main()
