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
		(anyHeavy(toString(parameters.platform)) as tmp_platform) > '',
		tmp_platform,
		'other'
	) as platform,
	if(
		(anyHeavy(toString(parameters.country)) as tmp_country) > '',
		tmp_country,
		'other'
	) as country,
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
	platform,
	country,
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
	    B.platform as platform,
	    B.country as country,
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
	group by date, utm_campaign, platform, country
	order by date, utm_campaign, platform, country
) as A
left join (
	select
		date,
		utm_campaign,
		platform,
		country,
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
	group by date, utm_campaign, platform, country
	order by date, utm_campaign, platform, country
) as B
using date, utm_campaign, platform, country
order by date''',
]



# MAU
sql_mau = [
'''drop table if exists vitrines.mau_dashboard''',
'''create table if not exists vitrines.mau_dashboard as
select
	toDate(toStartOfMonth(event_time)) as month_start,
	B.platform as platform,
	B.country as country,
	utm_campaign,
	uniqExact(user_id) as mau
from rikx.events as A
left join vitrines.user_data as B
using user_id
where event_time >= '2026-02-01'
  and event_time <= '2027-01-01'
  and app_version >= '0.30.3'
group by month_start, utm_campaign, platform, country
order by month_start, utm_campaign, platform, country desc''',
]



# WAU
sql_wau = [
'''drop table if exists vitrines.wau_dashboard''',
'''create table if not exists vitrines.wau_dashboard as
select
	week_start,
	platform,
	country,
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
group by week_start, utm_campaign, platform, country
order by week_start, utm_campaign, platform, country desc''',
]



# Sticky Factor
sql_sticky = [
'''drop table if exists vitrines.sticky_factor_dashboard''',
'''create table if not exists vitrines.sticky_factor_dashboard as
select
	d.date as date,
	d.utm_campaign as utm_campaign,
	platform,
	country,
	d.dau as dau,
	m.mau as mau,
	round(d.dau * 100.0 / m.mau, 2) as sticky_dau_mau
from vitrines.dau_dashboard as d
left join vitrines.mau_dashboard as m
on toStartOfMonth(d.date) = m.month_start
and d.utm_campaign = m.utm_campaign
and d.platform = m.platform
and d.country = m.country
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
		platform,
		country,
		utm_campaign,
		uniqExact(B.user_id) as retained_users
	from vitrines.user_data as A
	left join hourly_activity as B
	on A.user_id = B.user_id
	and B.activity_hour >= toStartOfHour(A.install_time)
	group by install_date, day, utm_campaign, platform, country
	order by install_date, day, utm_campaign, platform, country
)
select
	install_date,
	day,
	platform,
	country,
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
	if(
		(anyHeavy(toString(parameters.platform)) as tmp_platform) > '',
		tmp_platform,
		'other'
	) as platform,
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



sql_session_duration = [
'''drop table if exists vitrines.session_duration''',
'''create table if not exists vitrines.session_duration as
with events_with_sessions as (
	select
		user_id,
		event_time,
		previous_event_time,
		case when dateDiff('minute', previous_event_time, event_time) > 30
		    and dateDiff('minute', previous2_event_time, previous_event_time) < 30
			or previous_event_time is null
			then 1
			else 0
		end as is_new_session,
		sum(is_new_session) over (
			partition by user_id
			order by event_time
			rows between unbounded preceding and current row
		) as session_number
	from (
		select
			user_id,
			event_time,
			lag(event_time) over (partition by user_id order by event_time) as previous_event_time,
			lag(event_time, 2) over (partition by user_id order by event_time) as previous2_event_time
		from rikx.events
		where app_version != 'dashboards_test'
		  and event_time >= '2026-02-01'
		  and event_time <= '2027-01-01'
		  and app_version >= '0.30.3'
	)
)
select
	user_id,
	toDate(install_time) as install_date,
	platform,
	country,
	utm_campaign,
	session_number,
	dateDiff('second', min(event_time), max(event_time)) as session_duration_seconds,
	count(*) as events_in_session
from events_with_sessions as A
left join vitrines.user_data as B
using user_id
group by user_id, session_number, utm_campaign, platform, country, install_date
having session_duration_seconds > 0''',
]



sql_tutorial = [
'''drop table if exists vitrines.tutorial''',
'''create table if not exists vitrines.tutorial as
select
	toDate(install_time) as install_date,
	B.platform as platform,
	B.country as country,
	B.utm_campaign as utm_campaign,
	event_name as step_name,
	if(event_name == 'registered', -1, toInt32(extract(toString(parameters.step), '.*_0*([0-9]+)_.*'))) as step,
	count() as count
from rikx.events as A
left join vitrines.user_data as B
using user_id
where event_name in ('tutorial', 'registered')
  and app_version != 'dashboards_test'
  and event_time >= '2026-02-01'
  and event_time <= '2027-01-01'
  and app_version >= '0.30.3'
group by install_date, utm_campaign, platform, country, step, step_name
order by install_date, utm_campaign, platform, country, step, step_name''',
]



sql_scene_progression = [
'''drop table if exists vitrines.scene_progression''',
'''create table if not exists vitrines.scene_progression as
with battle_data as(
	select
		user_id,
		event_time,
		event_name,
		event_name = 'scene_unlock' as is_new_scene,
		lag(event_time) over (partition by user_id order by event_time) as previous_event_time
	from rikx.events
	where or(
		and(event_name = 'battle_finish', parameters.result = 'win'),
		event_name = 'scene_unlock'
	)
	  and app_version != 'dashboards_test'
	  and event_time >= '2026-02-01'
	  and event_time <= '2027-01-01'
	  and app_version >= '0.30.3'
	order by user_id, event_time
),
battle_stat as (
	select
		user_id,
		event_time,
		event_name,
		sum(is_new_scene) over (
			partition by user_id
			order by event_time
			rows between unbounded preceding and current row
		) as scene_number,
		if(previous_event_time > '1970-01-01 00:00:00', event_time - previous_event_time, 0) as delta
	from battle_data
)
select
	user_id,
	toDate(install_time) as install_date,
	platform,
	country,
	utm_campaign,
	scene_number,
	sum(delta) as time_to_reach,
	countIf(event_name = 'battle_finish') as battles
from battle_stat as A
left join vitrines.user_data as B
using user_id
group by user_id, install_date, utm_campaign, platform, country, scene_number
order by user_id, scene_number''',
]



sql_photo_progression = [
'''drop table if exists vitrines.photo_progression''',
'''create table if not exists vitrines.photo_progression as
with battle_data as(
	select
		user_id,
		event_time,
		event_name,
		event_name = 'new_photo' as is_new_photo,
		lag(event_time) over (partition by user_id order by event_time) as previous_event_time
	from rikx.events
	where or(
		and(event_name = 'battle_finish', parameters.result = 'win'),
		event_name = 'new_photo'
	)
	  and app_version != 'dashboards_test'
	  and event_time >= '2026-02-01'
	  and event_time <= '2027-01-01'
	  and app_version >= '0.30.3'
	order by user_id, event_time
),
battle_stat as (
	select
		user_id,
		event_time,
		event_name,
		sum(is_new_photo) over (
			partition by user_id
			order by event_time
			rows between unbounded preceding and current row
		) as photo_number,
		if(previous_event_time > '1970-01-01 00:00:00', event_time - previous_event_time, 0) as delta
	from battle_data
)
select
	user_id,
	toDate(install_time) as install_date,
	utm_campaign,
	platform,
	country,
	photo_number,
	sum(delta) as time_to_reach,
	countIf(event_name = 'battle_finish') as battles
from battle_stat as A
left join vitrines.user_data as B
using user_id
group by user_id, install_date, utm_campaign, platform, country, photo_number
order by user_id, photo_number''',
]



sql_battles_progression = [
'''drop table if exists vitrines.battles_progression''',
'''create table if not exists vitrines.battles_progression as
with battle_data as(
	select
		user_id,
		event_time,
		lag(event_time) over (partition by user_id order by event_time) as previous_event_time
	from rikx.events
	where event_name = 'battle_finish'
	  and app_version != 'dashboards_test'
	  and event_time >= '2026-02-01'
	  and event_time <= '2027-01-01'
	  and app_version >= '0.30.3'
	order by user_id, event_time
),
battle_stat as (
	select
		user_id,
		event_time,
		count(user_id) over (
			partition by user_id
			order by event_time
			rows between unbounded preceding and current row
		) as battle_number,
		if(previous_event_time > '1970-01-01 00:00:00', event_time - previous_event_time, 0) as delta
	from battle_data
)
select
	toDate(install_time) as install_date,
	utm_campaign,
	platform,
	country,
	battle_number,
	uniqExact(user_id) as users,
	sum(delta) as time_to_reach
from battle_stat as A
left join vitrines.user_data as B
using user_id
group by install_date, utm_campaign, platform, country, battle_number
order by battle_number
union all
select
	toDate(install_time) as install_date,
	utm_campaign,
	platform,
	country,
	0 as battle_number,
	uniqExact(user_id) as users,
	0 as time_to_reach
from vitrines.user_data
group by install_date, utm_campaign, platform, country, battle_number''',
]



sql_heroines_by_battle = [
'''drop table if exists vitrines.heroines_by_battle''',
'''create table if not exists vitrines.heroines_by_battle as
with battles as (
	select
		user_id,
		row_number(user_id) over (partition by user_id order by event_time) as battle_number,
		if(empty((parameters.heroines::Array(String) as arr)) > 0, ['_none'], arr) as heroines
	from rikx.events
	where event_name = 'battle_start'
	  and app_version != 'dashboards_test'
	  and event_time >= '2026-02-01'
	  and event_time <= '2027-01-01'
	  and app_version >= '0.30.3'
),
full_data as (
	select
		user_id,
		utm_campaign,
		platform,
		country,
		battle_number,
		arrayJoin(heroines) as heroine
	from battles as A
	left join vitrines.user_data as B
	using user_id
	order by user_id, battle_number, heroine
)
select
	utm_campaign,
	platform,
	country,
	battle_number,
	heroine,
	uniqExact(user_id) as users
from full_data
group by
	utm_campaign,
	platform,
	country,
	battle_number,
	heroine''',
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
	
    session_duration = PythonOperator(
        task_id='session_duration',
        python_callable=execute_queries,
        op_kwargs={"queries": sql_session_duration},
    )

    tutorial = PythonOperator(
        task_id='tutorial',
        python_callable=execute_queries,
        op_kwargs={"queries": sql_tutorial},
    )

    scene_progression = PythonOperator(
        task_id='scene_progression',
        python_callable=execute_queries,
        op_kwargs={"queries": sql_scene_progression},
    )

    photo_progression = PythonOperator(
        task_id='photo_progression',
        python_callable=execute_queries,
        op_kwargs={"queries": sql_photo_progression},
    )

    battles_progression = PythonOperator(
        task_id='battles_progression',
        python_callable=execute_queries,
        op_kwargs={"queries": sql_battles_progression},
    )

    heroines_by_battle = PythonOperator(
        task_id='heroines_by_battle',
        python_callable=execute_queries,
        op_kwargs={"queries": sql_heroines_by_battle},
    )



users_data >> dau >> mau >> wau >> sticky >> retention >> hourly_tech >> session_duration >> tutorial >> scene_progression >> photo_progression >> battles_progression >> heroines_by_battle
