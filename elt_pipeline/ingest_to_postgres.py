import os
try:
    import pandas as pd # pyright: ignore[reportMissingModuleSource]
except ImportError:
    raise ImportError("pandas is not installed. Install it using: pip install pandas")

try:
    from sqlalchemy import create_engine # pyright: ignore[reportMissingImports]
except ImportError:
    raise ImportError("sqlalchemy is not installed. Install it using: pip install sqlalchemy psycopg2-binary")

def run_ingestion(data_dir, **kwargs):
    # Target the exact file name directly inside the mounted folder
    excel_file_path = "/opt/airflow/data/FMN Data Engineer Assesment Dataset.xlsx"
    print(f"Directly accessing dataset at: {excel_file_path}")
    
    try:
        # Load the spreadsheet directly without loops
        df = pd.read_excel(excel_file_path)
        print(f"Successfully loaded spreadsheet into memory. Row count: {len(df)}")
        
        # Connect to Postgres container database
        # (Make sure the username, password, and port match your compose configurations)
        engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')
        
        # Save directly to the target raw table
        df.to_sql('raw_fmcg_sales', engine, if_exists='replace', index=False)
        print("Data ingestion into Postgres completed successfully!")
        
    except Exception as e:
        print(f"Database Ingestion failed: {e}")
        raise e