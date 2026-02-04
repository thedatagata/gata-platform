{%- macro extract_utm(column_name, utm_parameter) -%}
    LOWER(regexp_extract({{ column_name }}, '{{ utm_parameter }}=([^&]*)'))
{%- endmacro -%}

{%- macro get_date_dimension_spine(ads_ref, ga_ref) -%}
WITH analytics AS (
    SELECT DISTINCT 
        date, 
        COALESCE(source, '(not set)') as source, 
        COALESCE(medium, '(not set)') as medium, 
        COALESCE(campaign, '(not set)') as campaign 
    FROM {{ ref(ga_ref) }}
),
ads AS (
    SELECT DISTINCT 
        date, 
        COALESCE(utm_source, '(not set)') as source, 
        COALESCE(utm_medium, '(not set)') as medium, 
        COALESCE(utm_campaign, '(not set)') as campaign 
    FROM {{ ref(ads_ref) }}
),
unioned AS (
    SELECT * FROM analytics
    UNION
    SELECT * FROM ads
)
SELECT * FROM unioned
{%- endmacro -%}
