{% macro time_bucket(hour_col) %}
    case
        when {{ hour_col }} between 7 and 9   then 'morning_rush'
        when {{ hour_col }} between 10 and 15  then 'midday'
        when {{ hour_col }} between 16 and 19  then 'evening_rush'
        when {{ hour_col }} between 20 and 23  then 'night'
        else 'overnight'
    end
{% endmacro %}
