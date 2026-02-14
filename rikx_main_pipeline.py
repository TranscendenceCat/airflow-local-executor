from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.clickhouse.operators.clickhouse import ClickHouseOperator

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

    user_data_calc = ClickHouseOperator(
        task_id='user_data',
        clickhouse_conn_id='rikx_ch',
        sql="""show databases""",
    )

user_data_calc
