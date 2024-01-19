{{ config(materialized='view') }}

with all_aqi as 
(
select *, 'Kathmandu' as city_name  from {{ source("staging", "aqi_Kathmandu") }}
union all
select *, 'Delhi' as city_name from {{ source("staging", "aqi_Delhi") }}
union all
select *, 'London' as city_name from {{ source("staging", "aqi_London") }}
)
select *, 
{{ get_aqi_level_description('aqi') }} as aqi_level_description
 from all_aqi

{% if var('is_test_run', default=true) %}
limit 50    
{% endif %}