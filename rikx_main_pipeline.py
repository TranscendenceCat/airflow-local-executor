from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import clickhouse_driver

def execute_query(params):
    client = clickhouse_driver.Client(
        host='',
        port='',
        login='',
        password=''
    )
    print(client_execute(params['query']))

show_databases = '''show databases'''

with DAG(
    dag_id="rikx_main_pipeline",
    start_date=datetime(2025, 2, 1),
    description='Updating Datalens vitrines',
    schedule='0 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['clickhouse'],
):
    
    task_run = PythonOperator(
        task_id='show_databases',
        params={"query": show_databases},
        python_callable=execute_query,
    )

task_run
