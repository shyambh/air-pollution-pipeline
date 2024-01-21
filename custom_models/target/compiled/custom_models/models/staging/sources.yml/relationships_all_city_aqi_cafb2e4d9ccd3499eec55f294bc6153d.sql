
    
    

with child as (
    select city_name as from_field
    from `data-eng-practice007`.`dbt_core_models`.`all_city_aqi`
    where city_name is not null
),

parent as (
    select Capital City as to_field
    from `data-eng-practice007`.`dbt_core_models`.`country_coordinates_population`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


