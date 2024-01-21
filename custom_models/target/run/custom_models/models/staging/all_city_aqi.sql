

  create or replace view `data-eng-practice007`.`dbt_core_models`.`all_city_aqi`
  OPTIONS()
  as 

with all_aqi as 
(
select *, 'Kathmandu' as city_name  from `data-eng-practice007`.`weather_details`.`aqi_Kathmandu`
union all
select *, 'Delhi' as city_name from `data-eng-practice007`.`weather_details`.`aqi_Delhi`
union all
select *, 'London' as city_name from `data-eng-practice007`.`weather_details`.`aqi_London`
)
select *, 

    case aqi
        when 1 then 'Good'
        when 2 then 'Fair'
        when 3 then 'Moderate'
        when 4 then 'Poor'
        when 5 then 'Very Poor'
    end
 as aqi_level_description
 from all_aqi

;

