from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys

# Tell Airflow where to find your ETL script folder
sys.path.append('/opt/airflow')
from elt_pipeline.ingest_to_postgres import run_ingestion

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'fmn_fmcg_data_pipeline',
    default_args=default_args,
    description='FMN FMCG Data Pipeline',
    schedule_interval=None,
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_raw_to_postgres',
        python_callable=run_ingestion,
        op_kwargs={'data_dir': '/opt/airflow/data'}
    )

    dbt_run = BashOperator(
        task_id='dbt_run_transformations',
        bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir .',
    )
    ingest_task >> dbt_run