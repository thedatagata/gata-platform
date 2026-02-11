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

    SUM(COALESCE(prop_revenue, 0)) AS session_revenue,
    MAX(CASE WHEN prop_order_id IS NOT NULL THEN prop_order_id END) AS transaction_id

FROM sessioned
GROUP BY tenant_slug, source_platform, user_pseudo_id, session_number

{% endmacro %}


{#
  Engine: Mixpanel → Raw Events
  One row per event for fct_events
#}
{% macro engine_mixpanel_events(tenant_slug) %}

SELECT
    tenant_slug,
    source_platform,
    event_name,
    event_timestamp,
    user_pseudo_id,
    user_id,
    CAST(NULL AS VARCHAR) AS session_id,
    prop_order_id AS order_id,
    prop_revenue AS order_total,
    traffic_source,
    traffic_medium,
    traffic_campaign,
    geo_country,
    device_category
FROM {{ ref('int_' ~ tenant_slug ~ '__mixpanel_events') }}

{% endmacro %}


{#
  Engine: Mixpanel → Users (Identity Resolution)
  Aggregates analytics users and resolves to ecommerce customers via order_id
#}
{% macro engine_mixpanel_users(tenant_slug) %}

WITH events AS (
    SELECT * FROM {{ ref('fct_' ~ tenant_slug ~ '__events') }}
),

user_agg AS (
    SELECT
        tenant_slug,
        source_platform,
        user_pseudo_id,
        ANY_VALUE(user_id) AS user_id,
        MIN(event_timestamp) AS first_seen_at,
        MAX(event_timestamp) AS last_seen_at,
        COUNT(*) AS total_events,
        COUNT(DISTINCT session_id) AS total_sessions,
        ARG_MIN(geo_country, event_timestamp) AS first_geo_country,
        ARG_MIN(device_category, event_timestamp) AS first_device_category
    FROM events
    GROUP BY tenant_slug, source_platform, user_pseudo_id
),

purchase_links AS (
    SELECT DISTINCT user_pseudo_id, order_id
    FROM events
    WHERE order_id IS NOT NULL
),

orders AS (
    SELECT * FROM {{ ref('fct_' ~ tenant_slug ~ '__orders') }}
),

resolved AS (
    SELECT
        pl.user_pseudo_id,
        MAX(o.customer_email) AS customer_email,
        MAX(o.customer_id) AS customer_id
    FROM purchase_links pl
    INNER JOIN orders o ON CAST(pl.order_id AS VARCHAR) = CAST(o.order_id AS VARCHAR)
    GROUP BY pl.user_pseudo_id
)

SELECT
    ua.tenant_slug,
    ua.source_platform,
    ua.user_pseudo_id,
    ua.user_id,
    r.customer_email,
    r.customer_id,
    CASE WHEN r.customer_email IS NOT NULL THEN TRUE ELSE FALSE END AS is_customer,
    ua.first_seen_at,
    ua.last_seen_at,
    ua.total_events,
    ua.total_sessions,
    ua.first_geo_country,
    ua.first_device_category
FROM user_agg ua
LEFT JOIN resolved r ON ua.user_pseudo_id = r.user_pseudo_id

{% endmacro %}
