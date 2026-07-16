import os
import pandas as pd
from sqlalchemy import create_engine

def run_ingestion():
    print("🚀 Starting data ingestion process...")

    # 1. DYNAMIC FILE PATH
    # If running inside Docker/Airflow, it uses the container path.
    # If running locally on Windows, it automatically falls back to your local path.
    docker_path = "/opt/airflow/scripts/data/FMN Data Engineer Assesment Dataset.xlsx"
    local_path = "src/elt_pipeline/data/FMN Data Engineer Assesment Dataset.xlsx"
    
    file_path = docker_path if os.path.exists(docker_path) else local_path
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ Source file not found at: {file_path}")
        
    print(f"📂 Reading Excel file from: {file_path}")

    # 2. DYNAMIC DATABASE ENVIRONMENT
    # Reads environment variables (which Docker sets automatically).
    # If they don't exist (like on your Windows host), it defaults to your local setup!
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "fmcg_analytics")
    
    # Airflow/Docker will use the host 'postgres', local Windows will default to 'localhost'
    DB_HOST = os.getenv("DB_HOST", "localhost")

    # Create connection engine
    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(db_url)

    # 3. Extract and Load
    df = pd.read_excel(file_path)
    
    # (Your existing Data Validation & Cleaning code goes here...)
    # For example:
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    
    print("🔌 Connecting to PostgreSQL and loading data...")
    df.to_sql(
        name="staging_sales", 
        con=engine, 
        if_exists="replace", 
        index=False
    )
    print(f"✅ Successfully ingested {len(df)} rows into staging_sales table.")

if __name__ == "__main__":
    run_ingestion()