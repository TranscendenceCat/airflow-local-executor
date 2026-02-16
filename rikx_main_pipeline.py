from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import clickhouse_driver



# User data
sql_users_data = [
'''drop table if exists vitrines.user_data''',
'''create table if not exists vitrines.user_data as
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
group by user_id''',
]



# DAU
sql_dau = [
'''drop table if exists vitrines.dau_dashboard''',
'''create table if not exists vitrines.dau_dashboard as
select
	date,
	utm_campaign,
	dau,
	paying_users,
	payments,
	total_revenue,
	total_events,
	installs,
	logins
from (
	select 
	    toDate(event_time) as date,
	    utm_campaign,
	    uniqExact(user_id) as dau,
	    count(distinct case when event_name = 'payment_completed' then user_id end) as paying_users,
	    countIf(event_name = 'payment_completed') as payments,
	    sumIf(toDecimal32(parameters.amount, 4), event_name = 'payment_completed') as total_revenue,
	    count() as total_events
	from rikx.events as A
	left join vitrines.user_data as B
	using user_id
	where app_version != 'dashboards_test'
	  and event_time >= '2026-02-01'
	  and event_time <= '2027-01-01'
	  and app_version >= '0.30.3'
	group by date, utm_campaign
	order by date, utm_campaign
) as A
left join (
	select
		date,
		utm_campaign,
		uniqExact(user_id) as installs,
		uniqExact(if(has_logged, user_id, NULL)) as logins
	from (
		select
			user_id,
		    toDate(min(event_time)) as date,
		    countIf(event_name = 'login') > 0 as has_logged
		from rikx.events
		where app_version != 'dashboards_test'
		  and event_time >= '2026-02-01'
		  and event_time <= '2027-01-01'
		  and app_version >= '0.30.3'
		group by user_id
	) as A
	left join vitrines.user_data as B
	using user_id
	group by date, utm_campaign
	order by date, utm_campaign
) as B
using date, utm_campaign
order by date''',
]



# MAU
sql_mau = [
'''drop table if exists vitrines.mau_dashboard''',
'''create table if not exists vitrines.mau_dashboard as
select
	toDate(toStartOfMonth(event_time)) as month_start,
	utm_campaign,
	uniqExact(user_id) as mau
from rikx.events as A
left join vitrines.user_data as B
using user_id
where event_time >= '2026-02-01'
  and event_time <= '2027-01-01'
  and app_version >= '0.30.3'
group by month_start, utm_campaign
order by month_start, utm_campaign desc''',
]



# WAU
sql_wau = [
'''drop table if exists vitrines.wau_dashboard''',
'''create table if not exists vitrines.wau_dashboard as
select
	week_start,
	utm_campaign,
	uniqExact(user_id) as wau
from (
	select
		date_trunc('week', event_time) as week_start,
		user_id
	from rikx.events
	where app_version != 'dashboards_test'
	  and event_time >= '2026-02-01'
	  and event_time <= '2027-01-01'
	  and app_version >= '0.30.3'
	group by week_start, user_id
) as A
left join vitrines.user_data as B
using user_id
group by week_start, utm_campaign
order by week_start, utm_campaign desc''',
]



# Sticky Factor
sql_sticky = [
'''drop table if exists vitrines.sticky_factor_dashboard''',
'''create table if not exists vitrines.sticky_factor_dashboard as
select
	d.date as date,
	d.utm_campaign as utm_campaign,
	d.dau as dau,
	m.mau as mau,
	round(d.dau * 100.0 / m.mau, 2) as sticky_dau_mau
from vitrines.dau_dashboard as d
left join vitrines.mau_dashboard as m
on toStartOfMonth(d.date) = m.month_start
and d.utm_campaign = m.utm_campaign
order by d.date desc''',
]



# Retention
sql_retention = [
'''drop table if exists vitrines.retention_dashboard''',
'''create table if not exists vitrines.retention_dashboard as
with hourly_activity as (
	select 
		user_id,
		toStartOfHour(event_time) as activity_hour
	from rikx.events
	where app_version != 'dashboards_test'
	  and event_time >= '2026-02-01'
	  and event_time <= '2027-01-01'
	  and app_version >= '0.30.3'
	group by user_id, activity_hour
),
retention_calc as(
	select
		toDate(install_time) as install_date,
		floor((activity_hour - toStartOfHour(install_time)) / 86400, 0) as day,
		utm_campaign,
		uniqExact(B.user_id) as retained_users
	from vitrines.user_data as A
	left join hourly_activity as B
	on A.user_id = B.user_id
	and B.activity_hour >= toStartOfHour(A.install_time)
	group by install_date, day, utm_campaign
	order by install_date, day, utm_campaign
)
select
	install_date,
	day,
	utm_campaign,
	max(retained_users) over (partition by install_date, utm_campaign) as cohort_size,
	retained_users
from retention_calc''',
]



# Technical data
sql_hourly_tech = [
'''drop table if exists vitrines.hourly_tech_monitor''',
'''create table if not exists vitrines.hourly_tech_monitor as
select 
	toStartOfHour(event_time) as time,
	uniqExact(user_id) as active_users,
	count(distinct case when event_name = 'payment_completed' then user_id end) as paying_users,
	countIf(event_name = 'payment_completed') as payments,
	sumIf(toDecimal32(parameters.amount, 4), event_name = 'payment_completed') as total_revenue,
	count() as total_events,
	countIf(event_name = 'login') as login,
	countIf(event_name = 'battle_start') as battle_start,
	countIf(event_name = 'battle_finish') as battle_finish,
	countIf(event_name = 'heroine_upgrade') as heroine_upgrade,
	countIf(event_name = 'new_video') as new_video,
	countIf(event_name = 'new_photo') as new_photo,
	countIf(event_name = 'battlepass_gain') as battlepass_gain,
	countIf(event_name = 'payment_started') as payment_started,
	countIf(event_name = 'payment_completed') as payment_completed
from rikx.events
where app_version != 'dashboards_test'
  and event_time >= '2026-02-01'
  and event_time <= '2027-01-01'
  and app_version >= '0.30.3'
group by time
order by time''',
]






with DAG(
    dag_id="rikx_main_pipeline",
    start_date=datetime(2025, 2, 1),
    description='Updating Datalens vitrines',
    schedule='0 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['clickhouse'],
) as dag:

    def execute_queries(queries):
        client = clickhouse_driver.Client(
            host='138.68.75.226',
            port='9000',
            user='rikx',
            password='123456',
        )
        for query in queries:
            print(client.execute(query))



    users_data = PythonOperator(
        task_id='users_data',
        python_callable=execute_queries,
        op_kwargs={"queries": sql_users_data},
    )



    dau = PythonOperator(
        task_id='dau',
        python_callable=execute_queries,
        op_kwargs={"queries": sql_dau},
    )



    mau = PythonOperator(
        task_id='mau',
        python_callable=execute_queries,
        op_kwargs={"queries": sql_mau},
    )



    wau = PythonOperator(
        task_id='wau',
        python_callable=execute_queries,
        op_kwargs={"queries": sql_wau},
    )



    sticky = PythonOperator(
        task_id='sticky',
        python_callable=execute_queries,
        op_kwargs={"queries": sql_sticky},
    )



    retention = PythonOperator(
        task_id='retention',
        python_callable=execute_queries,
        op_kwargs={"queries": sql_retention},
    )



    hourly_tech = PythonOperator(
        task_id='hourly_tech',
        python_callable=execute_queries,
        op_kwargs={"queries": sql_hourly_tech},
    )





users_data >> dau >> mau >> wau >> sticky >> retention >> hourly_tech
