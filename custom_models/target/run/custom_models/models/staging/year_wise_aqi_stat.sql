

  create or replace view `data-eng-practice007`.`dbt_core_models`.`year_wise_aqi_stat`
  OPTIONS()
  as 

select extract(year from dt) as year, 
aqi_level_description, 
city_name,
count(*) as total_readings
from `data-eng-practice007`.`dbt_core_models`.`all_city_aqi`
group by year, city_name, aqi_level_description
order by year, city_name, aqi_level_description;

