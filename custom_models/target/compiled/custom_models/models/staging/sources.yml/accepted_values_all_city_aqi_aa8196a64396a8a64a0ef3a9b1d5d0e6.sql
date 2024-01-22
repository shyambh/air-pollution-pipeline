
    
    

with all_values as (

    select
        aqi_level_description as value_field,
        count(*) as n_records

    from `data-eng-practice007`.`dbt_core_models`.`all_city_aqi`
    group by aqi_level_description

)

select *
from all_values
where value_field not in (
    'Good','Fair','Moderate','Poor','Very Poor'
)


