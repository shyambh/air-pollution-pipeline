{{ config(materialized='view') }}

with rankeddata as (
	select city_name, dt as latest_time, aqi as latest_aqi , 
	row_number() over (partition by city_name order by dt desc) as row_num
	from {{ source("staging","all_city_aqi") }}
)
select city_name, latest_time, {{ get_aqi_level_description('latest_aqi') }} as aqi_level_description, latest_aqi from rankeddata where row_num = 1