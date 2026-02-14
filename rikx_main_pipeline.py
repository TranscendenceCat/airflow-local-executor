from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.clickhouse.operators.clickhouse import ClickHouseOperator

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook

from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

with DAG(
    'rikx_main_pipeline',
    default_args=default_args,
    description='Updating Datalens vitrines',
    schedule='0 * * * *',
    start_date=datetime(2025, 2, 1),
    catchup=False,
    max_active_runs=1,
    tags=['clickhouse'],
) as dag:
    
    # user_data_calc = ClickHouseOperator(
    #     task_id='user_data',
    #     clickhouse_conn_id='rikx_ch',
    #     sql="""show databases""",
    # )

    # @task(task_id="print_the_context")
    def HookCallable():
        try:
            ch_conn = ClickHouseHook(clickhouse_conn_id='rikx_ch')
            q = 'show databases'
            data = list(ch_conn.run(q))
        except Exception as error:
            print("An exception occurred:", error)
        return data

    task_run = (task_id="user_data_calc", python_callable=HookCallable)

task_run
    # user_data_calc = EmptyOperator(task_id="start_task")

# user_data_calc
