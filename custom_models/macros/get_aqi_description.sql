-- This macro returns the description of the AQI

{% macro get_aqi_level_description(aqi_level) %}
    case {{ aqi_level }}
        when 1 then 'Good'
        when 2 then 'Fair'
        when 3 then 'Moderate'
        when 4 then 'Poor'
        when 5 then 'Very Poor'
    end
{% endmacro %}