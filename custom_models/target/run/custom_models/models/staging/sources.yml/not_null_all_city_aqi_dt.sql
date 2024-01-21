select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select dt
from `data-eng-practice007`.`dbt_core_models`.`all_city_aqi`
where dt is null



      
    ) dbt_internal_test