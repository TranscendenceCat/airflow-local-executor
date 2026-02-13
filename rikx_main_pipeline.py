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
    schedule='0 * * * *',
    start_date=datetime(2025, 2, 1),
    catchup=False,
    max_active_runs=1,
    tags=['clickhouse'],
) as dag:

    user_data_calc = ClickHouseOperator(
        task_id='user_data',
        clickhouse_conn_id='rikx_ch',
        sql=[
            """drop table if exists vitrines.user_data""",
            """create table vitrines.user_data as
            select
            	user_id,
            	min(event_time) as install_time,
            	if(
            		(anyIf(toString(parameters.get_request.utm_campaign), event_name = 'registered') as campaign) > '',
            		campaign,
            		'other'
            	) as utm_campaign
            from rikx.events
            where app_version != 'dashboards_test'
              and event_time >= '2026-02-01'
              and event_time <= '2027-01-01'
              and app_version >= '0.30.3'
            group by user_id""",
        ],
        database='default',
    )

user_data_calc
