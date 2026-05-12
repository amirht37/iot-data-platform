from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline import run_pipeline

default_args = {
    'owner': 'amrire',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='iot_sensor_pipeline',
    default_args=default_args,
    description='IoT Sensor ETL - Incremental with Quarantine',
    schedule='*/5 * * * *',      
    start_date=datetime(2025, 1, 1),
    catchup=False, 
    max_active_runs=1, 
    tags=['iot', 'etl', 'sensor'],
) as dag:

    run_pipeline_task = PythonOperator(
        task_id='run_iot_etl_pipeline',
        python_callable=run_pipeline,
    )
