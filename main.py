import pandas as pd
from pymongo import MongoClient

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "CannabisDB"
COLLECTION_NAME = "Strains"

# File Paths
CSV_FILE = "cannabis_strains.csv"
PARQUET_FILE = "cannabis_strains.parquet"

# 1. Capture Data from CSV
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

# 2. Validate and Clean Data
def clean_data(data):
    print("Iniciando limpeza de dados...")
    try:
        # Convert 'Effects' to a list
        data['Effects'] = data['Effects'].apply(lambda x: x.split(",") if pd.notnull(x) else [])
        # Convert 'Flavor' to a list
        data['Flavor'] = data['Flavor'].apply(lambda x: x.split(",") if pd.notnull(x) else [])
        # Remove leading/trailing whitespaces in lists
        data['Effects'] = data['Effects'].apply(lambda effects: [effect.strip() for effect in effects])
        data['Flavor'] = data['Flavor'].apply(lambda flavors: [flavor.strip() for flavor in flavors])
        # Fill missing values in 'Description' with empty strings
        data['Description'] = data['Description'].fillna("")
        print("Dados limpos com sucesso!")
        return data
    except Exception as e:
        print(f"Erro na limpeza dos dados: {e}")
        return None

# 3. Save to Parquet
def save_to_parquet(data, file_path):
    print("Salvando dados no formato Parquet...")
    try:
        data.to_parquet(file_path, index=False)
        print("Dados salvos no arquivo Parquet!")
    except Exception as e:
        print(f"Erro ao salvar dados no Parquet: {e}")

# 4. Insert Data into MongoDB
def ingest_to_mongodb(data):
    print("Conectando ao MongoDB...")
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        # Insert data into MongoDB
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

        # Example: Top-rated strains
        print("Top 5 cepas mais bem avaliadas:\n")
        top_rated = collection.find().sort("Rating", -1).limit(5)
        
        # Print each strain in a more structured and readable way
        for strain in top_rated:
            print("-" * 60)  # Separator line for better visual distinction
            print(f"Strain: {strain['Strain']}")
            print(f"Type: {strain['Type']}")
            print(f"Rating: {strain['Rating']}")
            print(f"Effects: {', '.join(strain['Effects']) if strain['Effects'] else 'No effects listed'}")
            print(f"Flavor: {', '.join(strain['Flavor']) if strain['Flavor'] else 'No flavors listed'}")
            print(f"Description: {strain['Description'][:250]}...")  # Show a snippet of the description
            print("-" * 60)  # Separator line to end each strain's info
        client.close()
    except Exception as e:
        print(f"Erro ao executar consultas no MongoDB: {e}")

# Main Workflow
def main():
    # Load data
    data = capture_data(CSV_FILE)
    if data is None:
        return

    # Clean and validate data
    cleaned_data = clean_data(data)
    if cleaned_data is None:
        return

    # Save to Parquet
    save_to_parquet(cleaned_data, PARQUET_FILE)

    # Ingest into MongoDB
    ingest_to_mongodb(cleaned_data)

    # Run basic queries
    query_mongodb()

if __name__ == "__main__":
    main()
