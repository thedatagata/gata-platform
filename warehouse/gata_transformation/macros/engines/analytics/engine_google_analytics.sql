{# 
  Engine: Google Analytics → Sessions
  Reads: int_{tenant_slug}__google_analytics_events
  Sessionizes events using 30-min inactivity window
  Accepts conversion_events list for is_conversion_session flag
#}
{% macro engine_google_analytics_sessions(tenant_slug, conversion_events=[]) %}

WITH events AS (
    SELECT
        *,
        -- Build session boundaries: new session if gap > 30 min from prior event for same user
        CASE 
            WHEN LAG(event_timestamp) OVER (
                PARTITION BY user_pseudo_id 
                ORDER BY event_timestamp
            ) IS NULL THEN 1
            WHEN event_timestamp - LAG(event_timestamp) OVER (
                PARTITION BY user_pseudo_id 
                ORDER BY event_timestamp
            ) > 1800000000  -- 30 min in microseconds
            THEN 1
            ELSE 0
        END AS is_new_session
    FROM {{ ref('int_' ~ tenant_slug ~ '__google_analytics_events') }}
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
    (MAX(event_timestamp) - MIN(event_timestamp)) / 1000000 AS session_duration_seconds,
    COUNT(*)                        AS events_in_session,

    -- First-touch attribution
    ARG_MIN(traffic_source, event_timestamp)     AS traffic_source,
    ARG_MIN(traffic_medium, event_timestamp)     AS traffic_medium,
    ARG_MIN(traffic_campaign, event_timestamp)   AS traffic_campaign,

    -- Geo / device (first event in session)
    ARG_MIN(geo_country, event_timestamp)        AS geo_country,
    ARG_MIN(device_category, event_timestamp)    AS device_category,

    -- Conversion flag
    {% if conversion_events | length > 0 %}
    MAX(CASE WHEN event_name IN ({% for evt in conversion_events %}'{{ evt }}'{% if not loop.last %}, {% endif %}{% endfor %}) THEN TRUE ELSE FALSE END) AS is_conversion_session,
    {% else %}
    FALSE AS is_conversion_session,
    {% endif %}

    -- Ecommerce (sum of purchase revenue in session)
    SUM(COALESCE(purchase_revenue, 0)) AS session_revenue,
    MAX(transaction_id)                AS transaction_id

FROM sessioned
GROUP BY tenant_slug, source_platform, user_pseudo_id, session_number

{% endmacro %}
