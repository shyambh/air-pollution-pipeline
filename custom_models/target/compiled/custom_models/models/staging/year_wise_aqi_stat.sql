

select extract(year from dt) as year, 
aqi_level_description, 
city_name,
count(*) as total_readings
from `data-eng-practice007`.`dbt_core_models`.`all_city_aqi_with_desc`
group by year, city_name, aqi_level_description
order by year, city_name, aqi_level_description