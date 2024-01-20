{{ config(materialized='view') }}

select extract(year from dt) as year, 
aqi_level_description, 
city_name,
count(*) as total_readings
from {{ ref("all_city_aqi") }}
group by year, city_name, aqi_level_description
order by year, city_name, aqi_level_description