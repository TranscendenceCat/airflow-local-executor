from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.clickhouse.operators.clickhouse import ClickHouseOperator
from airflow.operators.dummy import DummyOperator

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
    schedule_interval=None,  # Set to None for manual trigger, or use cron expression
    start_date=datetime(2025, 2, 1),
    catchup=False,
    tags=['clickhouse'],
) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    # First ClickHouse query
    query_1 = ClickHouseOperator(
        task_id='first_query',
        clickhouse_conn_id='rikx_ch',
        sql="""
            drop table vitrines.user_data;
            create table vitrines.user_data as
            select
            	user_id,
            	min(event_time) as install_time,
            	if(
            		(anyIf(toString(parameters.get_request.utm_campaign), event_name = 'registered') as campaign) > '',
            		campaign,
            		'other'
            	) as utm_campaign
            --	anyIf(toString(parameters.get_request.zID), event_name = 'registered') as zID,
            --	anyIf(toString(parameters.get_request.happs), event_name = 'registered') as happs,
            --	anyIf(toString(parameters.get_request.crID), event_name = 'registered') as crID,
            --	anyIf(toString(parameters.get_request.land), event_name = 'registered') as land,
            --	anyIf(toString(parameters.get_request.utm_content), event_name = 'registered') as utm_content,
            --	anyIf(toString(parameters.get_request.bnid), event_name = 'registered') as bnid
            from rikx.events
            where app_version != 'dashboards_test'
              and event_time >= '2026-02-01'
              and event_time <= '2027-01-01'
              and app_version >= '0.30.3'
            group by user_id;
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
    start >> query_1 >> end # >> query_2 >> query_3
