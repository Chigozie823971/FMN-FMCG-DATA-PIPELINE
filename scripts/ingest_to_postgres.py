import os
import pandas as pd
from sqlalchemy import create_engine

def run_ingestion():
    print("Starting data ingestion process...")
    
    # Database connection parameters
    DB_USER = "postgres"
    DB_PASSWORD = "postgres"
    DB_HOST = "postgres" # Uses docker-compose network bridge
    DB_PORT = "5432"
    DB_NAME = "fmcg_analytics"
    
    # Create connection engine
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    # Path to raw file (mock pathway within container)
    file_path = "/opt/airflow/scripts/data/raw_sales.csv"
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Source file not found at {file_path}")
        
    # Extract using Pandas
    df = pd.read_csv(file_path)
    
    # Data Validation & Cleaning
    df.drop_duplicates(inplace=True)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    
    # Load into Postgres Staging Schema
    df.to_sql('staging_sales', con=engine, schema='public', if_exists='replace', index=False)
    print(f"Successfully ingested {len(df)} rows into staging_sales table.")

if __name__ == "__main__":
    run_ingestion()