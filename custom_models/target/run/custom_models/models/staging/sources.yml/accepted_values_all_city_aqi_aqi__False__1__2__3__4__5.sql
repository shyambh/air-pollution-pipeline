select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        aqi as value_field,
        count(*) as n_records

    from `data-eng-practice007`.`dbt_core_models`.`all_city_aqi`
    group by aqi

)

select *
from all_values
where value_field not in (
    1,2,3,4,5
)



      
    ) dbt_internal_test