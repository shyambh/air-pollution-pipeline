{{ config(materialized='view') }}

select *, 
{{ get_aqi_level_description('aqi') }} as aqi_level_description
from {{ source("staging","all_city_aqi") }}

{% if var('is_test_run', default=true) %}
limit 50    
{% endif %}