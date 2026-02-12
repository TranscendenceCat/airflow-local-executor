from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.clickhouse.operators.clickhouse import ClickHouseOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'rikx_main_pipeline',
    default_args=default_args,
    description='Updating Datalens vitrines',
    schedule='0 * * * *',  # Set to None for manual trigger, or use cron expression
    start_date=datetime(2025, 2, 1),
    catchup=False,
    max_active_runs=1,
    tags=['clickhouse'],
) as dag:

    # First ClickHouse query
    user_data = ClickHouseOperator(
        task_id='user_data',
        clickhouse_conn_id='rikx_ch',
        sql="""
drop table if exists vitrines.user_data;
        """,
        database='default',  # Your database name
    )

    # Second ClickHouse query
    # query_2 = ClickHouseOperator(
    #     task_id='second_query',
    #     clickhouse_conn_id='clickhouse_default',
    #     sql="""
    #         INSERT INTO summary_table (date, count)
    #         SELECT 
    #             today(),
    #             COUNT(*) 
    #         FROM your_table 
    #         WHERE date = today()
    #     """,
    #     database='default',
    # )

    # Third ClickHouse query
    # query_3 = ClickHouseOperator(
    #     task_id='third_query',
    #     clickhouse_conn_id='clickhouse_default',
    #     sql="""
    #         SELECT 
    #             date,
    #             SUM(amount) as total_amount
    #         FROM transactions 
    #         WHERE date >= today() - INTERVAL 7 DAY
    #         GROUP BY date
    #         ORDER BY date
    #     """,
    #     database='default',
    # )

    # Set task dependencies - sequential execution
user_data # >> query_2 >> query_3
