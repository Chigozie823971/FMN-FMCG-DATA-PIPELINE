import os
import glob
import pandas as pd
from sqlalchemy import create_engine

# Database Connection Environmental Settings
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "fmn_analytics")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

def get_db_engine():
    """Generates an active connection pool engine to PostgreSQL."""
    return create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}")

def clean_column_names(df):
    """Cleans messy or spaces in column headers into clear snake_case formatting."""
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(' ', '_')
        .str.replace('(', '')
        .str.replace(')', '')
        .str.replace('%', 'pct')
    )
    return df

def run_ingestion(data_dir="/opt/airflow/data"):
    """
    Finds either the multi-sheet excel file or the separate CSV file components,
    cleans the schemas, applies validation checks, and overwrites the raw staging schema layer.
    """
    engine = get_db_engine()
    
    # Initialize Target Raw Database Schema Layer
    with engine.connect() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw_source;")
    
    # Mapping sheet/component logic to raw table targets
    dataset_map = {
        'Transactions': 'raw_transactions',
        'Products': 'raw_products',
        'Distributors': 'raw_distributors',
        'Salespersons': 'raw_salespersons',
        'Monthly_Targets': 'raw_monthly_targets',
        'Date_Table': 'raw_date_table'
    }
    
    print(f"Beginning pipeline ingestion loop looking inside target directory: {data_dir}")
    
    for key_name, table_target in dataset_map.items():
        df = None
        
        # Look for explicit CSV match strings first
        csv_pattern = os.path.join(data_dir, f"*{key_name}*.csv")
        matched_files = glob.glob(csv_pattern)
        
        if matched_files:
            print(f"Found CSV segment file for {key_name}: {matched_files[0]}")
            df = pd.read_csv(matched_files[0])
        else:
            # Fallback to check if a monolithic workbook exists
            excel_path = os.path.join(data_dir, "FMN Data Engineer Assesment Dataset.xlsx")
            if os.path.exists(excel_path):
                print(f"Reading {key_name} from primary Excel workbook sheet.")
                df = pd.read_excel(excel_path, sheet_name=key_name)
        
        if df is None:
            print(f"⚠️ Warning: Could not locate data source for component {key_name}. Skipping.")
            continue
            
        # Standardize and map columns
        df = clean_column_names(df)
        
        # --- Data Validation & Cleaning Layer ---
        if table_target == 'raw_transactions':
            # Drop entries missing vital key identifiers
            df = df.dropna(subset=['transaction_id'])
            # Cast raw object dates explicitly to datetime sequences
            df['transaction_date'] = pd.to_datetime(df['transaction_date'])
            # Fix missing distributor relationships safely using an unknown tag fallback
            df['distributor_id'] = df['distributor_id'].fillna('UNKNOWN_DIST')
            
        elif table_target == 'raw_monthly_targets':
            df = df.dropna(subset=['record_id'])
            # Safely calculate missing target metric percentages dynamically if empty
            if 'achievement_pct' not in df.columns or df['achievement_pct'].isnull().all():
                df['achievement_pct'] = (df['actual_revenue_ngn'] / df['target_revenue_ngn']) * 100.0
                
        # --- Incremental / Complete Replacement Pipeline Logic ---
        # Using replace here guarantees the target raw tables maintain proper structural alignments on runs
        df.to_sql(table_target, engine, schema='raw_source', if_exists='replace', index=False)
        print(f" Successfully loaded {len(df)} records into target table: raw_source.{table_target}")

if __name__ == "__main__":
    # If testing locally outside Docker, use your local 'data' directory
    local_data_path = "data" if os.path.exists("data") else "."
    run_ingestion(data_dir=local_data_path)