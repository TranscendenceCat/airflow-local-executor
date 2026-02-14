from datetime import datetime, timedelta
from airflow import DAG
# from airflow.providers.clickhouse.operators.clickhouse import ClickHouseOperator

from airflow.providers.standard.operators.empty import EmptyOperator
# from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook

# from airflow.operators.python import PythonOperator

import logging

logger = logging.getLogger(__name__)
# logger.info("This is a log message")

with DAG(
    dag_id="rikx_main_pipeline",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
    start_date=datetime(2025, 2, 1),
    description='Updating Datalens vitrines',
    schedule='0 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['clickhouse'],
):

    task_run = EmptyOperator(task_id="task")
    
    # user_data_calc = ClickHouseOperator(
    #     task_id='user_data',
    #     clickhouse_conn_id='rikx_ch',
    #     sql="""show databases""",
    # )

    # @task(task_id="print_the_context")
    # def HookCallable():
    #     logger.info("This is a log message in hook")
    #     try:
    #         # ch_conn = ClickHouseHook(clickhouse_conn_id='rikx_ch')
    #         q = 'show databases'
    #         # data = list(ch_conn.run(q))
    #     except Exception as error:
    #         print("An exception occurred:", error)
    #     logger.info("This is a log message after hook")
    #     return data

    # task_run = PythonOperator(task_id="user_data_calc", python_callable=HookCallable)

task_run
