from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from etl import run

default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 20),  
    'retries': 1,
}

with DAG(
    'automate_etl_daily',
    default_args=default_args,
    schedule_interval='0 3 * * *',  # Daily at 3:00 AM
    catchup=False,
) as dag:
    run_task = PythonOperator(
        task_id='daily_etl',
        python_callable=run,
    )

    run_task