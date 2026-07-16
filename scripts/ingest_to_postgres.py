import os
import pandas as pd
from sqlalchemy import create_engine

def run_ingestion():
    print("🚀 Starting data ingestion process...")

    # 1. Dynamic File Path Selection
    docker_path = "/opt/airflow/scripts/data/FMN Data Engineer Assesment Dataset.xlsx"
    local_path = "src/elt_pipeline/data/FMN Data Engineer Assesment Dataset.xlsx"
    
    file_path = docker_path if os.path.exists(docker_path) else local_path
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ Source file not found at: {file_path}")
        
    print(f"📂 Reading Excel workbook from: {file_path}")

    # 2. Dynamic Database Environment Setup
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "fmcg_analytics")
    DB_HOST = os.getenv("DB_HOST", "localhost")

    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(db_url)

    # 3. Load Excel Workbook and Iterate Through All Sheets
    excel_file = pd.ExcelFile(file_path)
    
    print(f"📊 Found {len(excel_file.sheet_names)} sheets: {excel_file.sheet_names}")
    
    for sheet_name in excel_file.sheet_names:
        print(f"\n⏳ Processing sheet: '{sheet_name}'...")
        
        # Read the specific sheet
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        
        # Clean column names (lowercase, strip spaces, replace spaces with underscores)
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
        
        # Create a clean database table name from the sheet name
        table_name = sheet_name.strip().lower().replace(' ', '_')
        
        print(f"🔌 Loading {len(df)} rows into table '{table_name}'...")
        
        # Ingest to Postgres
        df.to_sql(
            name=table_name, 
            con=engine, 
            if_exists="replace", 
            index=False
        )
        print(f"✅ Successfully loaded '{table_name}'!")

    print("\n🎉 All sheets have been successfully ingested into PostgreSQL!")

if __name__ == "__main__":
    run_ingestion()