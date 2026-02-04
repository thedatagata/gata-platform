{% macro engine_google_analytics(tenant_slug, logic_config={}) %}
{#
    Engine: Google Analytics (base extraction + session construction)
    Input: platform_mm__google_analytics_api_v1_events
    
    Mock data fields: event_name, event_date (VARCHAR), event_timestamp (BIGINT),
                      user_pseudo_id, traffic_source__source, traffic_source__medium,
                      traffic_source__campaign, geo__country, geo__city,
                      ecommerce__transaction_id, ecommerce__value, ecommerce__currency
    
    Note: Mock data lacks ga_session_id and page_location.
          Sessions are constructed from user_pseudo_id + 30-min inactivity window.
          Conversion detection uses event_name (e.g., 'purchase') from logic_config.
#}
{%- set conversion_events = logic_config.get('conversion_events', ['purchase']) -%}

WITH base AS (
    SELECT
        tenant_slug,
        CAST(strptime(raw_data_payload->>'event_date', '%Y%m%d') AS DATE) AS date,
        raw_data_payload->>'user_pseudo_id' as user_pseudo_id,
        CAST(raw_data_payload->>'event_timestamp' AS BIGINT) as event_timestamp,
        raw_data_payload->>'event_name' as event_name,
        COALESCE(LOWER(raw_data_payload->>'traffic_source__source'), '(not set)') as source,
        COALESCE(LOWER(raw_data_payload->>'traffic_source__medium'), '(not set)') as medium,
        COALESCE(LOWER(raw_data_payload->>'traffic_source__campaign'), '(not set)') as campaign,
        raw_data_payload->>'geo__country' as geo_country,
        raw_data_payload->>'geo__city' as geo_city,
        raw_data_payload->>'ecommerce__transaction_id' as transaction_id,
        CAST(raw_data_payload->>'ecommerce__value' AS DOUBLE) as transaction_value,
        raw_data_payload->>'ecommerce__currency' as transaction_currency,
        raw_data_payload
    FROM {{ ref('platform_mm__google_analytics_api_v1_events') }}
    WHERE tenant_slug = '{{ tenant_slug }}'
),

{# Construct sessions via 30-minute inactivity window #}
with_session_boundaries AS (
    SELECT *,
        CASE 
            WHEN event_timestamp - LAG(event_timestamp) OVER (
                PARTITION BY user_pseudo_id ORDER BY event_timestamp
            ) > 1800000000  {# 30 min in microseconds #}
            OR LAG(event_timestamp) OVER (
                PARTITION BY user_pseudo_id ORDER BY event_timestamp
            ) IS NULL
            THEN 1
            ELSE 0
        END as is_new_session
    FROM base
),

with_session_id AS (
    SELECT *,
        SUM(is_new_session) OVER (
            PARTITION BY user_pseudo_id ORDER BY event_timestamp
            ROWS UNBOUNDED PRECEDING
        ) as session_number,
        user_pseudo_id || '_' || CAST(SUM(is_new_session) OVER (
            PARTITION BY user_pseudo_id ORDER BY event_timestamp
            ROWS UNBOUNDED PRECEDING
        ) AS VARCHAR) as ga_session_id
    FROM with_session_boundaries
),

enriched AS (
    SELECT 
        *,
        FIRST_VALUE(source IGNORE NULLS) OVER (
            PARTITION BY user_pseudo_id, ga_session_id
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS session_source,
        FIRST_VALUE(medium IGNORE NULLS) OVER (
            PARTITION BY user_pseudo_id, ga_session_id
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS session_medium,
        FIRST_VALUE(campaign IGNORE NULLS) OVER (
            PARTITION BY user_pseudo_id, ga_session_id
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS session_campaign,
        CASE WHEN event_name IN (
            {% for e in conversion_events %}'{{ e }}'{% if not loop.last %}, {% endif %}{% endfor %}
        ) THEN TRUE ELSE FALSE END AS is_conversion_event
    FROM with_session_id
)

SELECT * FROM enriched
{% endmacro %}
