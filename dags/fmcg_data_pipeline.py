from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine

def load_fmcg_data_to_postgres():
    print("Connecting to target database container: fmn-fmcg-data-pipeline-postgres-1...")
    
    # Establish connection to your running postgres-1 container
    engine = create_engine("postgresql://postgres:postgres@postgres:5432/fmcg_analytics")
    
    # Structural FMCG sales dataset to populate the target database
    data = {
        'transaction_id': [1001, 1002, 1003, 1004],
        'product_name': ['Golden Penny Flour', 'Noodles', 'Golden Penny Flour', 'Sugar'],
        'customer_id': [55, 62, 55, 41],
        'quantity': [10, 5, 12, 20],
        'total_amount': [15000, 2500, 18000, 30000],
        'updated_at': [str(datetime.now())] * 4
    }
    df = pd.DataFrame(data)
    
    # Write dataframe straight into postgres-1 target database
    print("Streaming structured tables into staging_sales...")
    df.to_sql('staging_sales', con=engine, if_exists='replace', index=False)
    print("Database population complete! postgres-1 is no longer empty.")

with DAG(
    'populate_fmcg_postgres',
    start_date=datetime(2026, 7, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    run_ingestion = PythonOperator(
        task_id='load_fmcg_data',
        python_callable=load_fmcg_data_to_postgres
    )