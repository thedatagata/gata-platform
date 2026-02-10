{# 
  Engine: Mixpanel → Sessions
  Reads: int_{tenant_slug}__mixpanel_events
  Mixpanel uses distinct_id, sessionized via 30-min window
#}
{% macro engine_mixpanel_sessions(tenant_slug, conversion_events=[]) %}

WITH events AS (
    SELECT
        *,
        CASE 
            WHEN LAG(event_timestamp) OVER (
                PARTITION BY user_pseudo_id 
                ORDER BY event_timestamp
            ) IS NULL THEN 1
            WHEN event_timestamp - LAG(event_timestamp) OVER (
                PARTITION BY user_pseudo_id 
                ORDER BY event_timestamp
            ) > 1800000  -- 30 min in milliseconds
            THEN 1
            ELSE 0
        END AS is_new_session
    FROM {{ ref('int_' ~ tenant_slug ~ '__mixpanel_events') }}
),

sessioned AS (
    SELECT
        *,
        SUM(is_new_session) OVER (
            PARTITION BY user_pseudo_id 
            ORDER BY event_timestamp
        ) AS session_number
    FROM events
)

SELECT
    tenant_slug,
    source_platform,
    user_pseudo_id || '-' || CAST(session_number AS VARCHAR) AS session_id,
    ANY_VALUE(user_pseudo_id)       AS user_pseudo_id,
    ANY_VALUE(user_id)              AS user_id,
    MIN(event_timestamp)            AS session_start_ts,
    MAX(event_timestamp)            AS session_end_ts,
    (MAX(event_timestamp) - MIN(event_timestamp)) / 1000 AS session_duration_seconds,
    COUNT(*)                        AS events_in_session,

    ARG_MIN(traffic_source, event_timestamp)     AS traffic_source,
    ARG_MIN(traffic_medium, event_timestamp)     AS traffic_medium,
    ARG_MIN(traffic_campaign, event_timestamp)   AS traffic_campaign,

    ARG_MIN(geo_country, event_timestamp)        AS geo_country,
    ARG_MIN(device_category, event_timestamp)    AS device_category,

    {% if conversion_events | length > 0 %}
    MAX(CASE WHEN event_name IN ({% for evt in conversion_events %}'{{ evt }}'{% if not loop.last %}, {% endif %}{% endfor %}) THEN TRUE ELSE FALSE END) AS is_conversion_session,
    {% else %}
    FALSE AS is_conversion_session,
    {% endif %}

    CAST(0 AS DOUBLE) AS session_revenue,
    CAST(NULL AS VARCHAR) AS transaction_id

FROM sessioned
GROUP BY tenant_slug, source_platform, user_pseudo_id, session_number

{% endmacro %}
