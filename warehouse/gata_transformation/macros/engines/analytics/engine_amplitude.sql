{# 
  Engine: Amplitude → Sessions
  Reads: int_{tenant_slug}__amplitude_events
  Amplitude has session_id natively
#}
{% macro engine_amplitude_sessions(tenant_slug, conversion_events=[]) %}

SELECT
    tenant_slug,
    source_platform,
    session_id,
    user_pseudo_id,
    user_id,
    MIN(event_timestamp)            AS session_start_ts,
    MAX(event_timestamp)            AS session_end_ts,
    (MAX(event_timestamp) - MIN(event_timestamp)) / 1000 AS session_duration_seconds,
    COUNT(*)                        AS events_in_session,

    ARG_MIN(traffic_source, event_timestamp)     AS traffic_source,
    ARG_MIN(traffic_medium, event_timestamp)     AS traffic_medium,
    ARG_MIN(traffic_campaign, event_timestamp)   AS traffic_campaign,

    ARG_MIN(geo_country, event_timestamp)        AS geo_country,
    ARG_MIN(device_category, event_timestamp)    AS device_category,

    -- ===== FUNNEL ANALYSIS =====
    -- Deepest funnel step reached (1-indexed, 0 = no funnel activity)
    {{ build_funnel_max_step() }} AS funnel_max_step,

    -- Per-step event counts (one column per funnel step)
    {{ build_funnel_pivot_columns() }},

    -- Conversion event count within session
    SUM(CASE WHEN event_name IN (
        {%- for evt in conversion_events %}'{{ evt }}'{% if not loop.last %}, {% endif %}{% endfor -%}
        {%- if conversion_events | length == 0 %}'__none__'{% endif -%}
    ) THEN 1 ELSE 0 END) AS funnel_conversion_count,

    -- Boolean conversion flag
    SUM(CASE WHEN event_name IN (
        {%- for evt in conversion_events %}'{{ evt }}'{% if not loop.last %}, {% endif %}{% endfor -%}
        {%- if conversion_events | length == 0 %}'__none__'{% endif -%}
    ) THEN 1 ELSE 0 END) > 0 AS is_conversion_session,

    CAST(0 AS DOUBLE) AS session_revenue,
    CAST(NULL AS VARCHAR) AS transaction_id

FROM {{ ref('int_' ~ tenant_slug ~ '__amplitude_events') }}
WHERE session_id IS NOT NULL
GROUP BY tenant_slug, source_platform, session_id, user_pseudo_id, user_id

{% endmacro %}


{#
  Engine: Amplitude → Raw Events
  One row per event for fct_events
#}
{% macro engine_amplitude_events(tenant_slug) %}

SELECT
    tenant_slug,
    source_platform,
    event_name,
    event_timestamp,
    user_pseudo_id,
    user_id,
    session_id,
    order_id,
    order_total,
    traffic_source,
    traffic_medium,
    traffic_campaign,
    geo_country,
    device_category
FROM {{ ref('int_' ~ tenant_slug ~ '__amplitude_events') }}

{% endmacro %}


{#
  Engine: Amplitude → Users (Identity Resolution)
  Aggregates analytics users and resolves to ecommerce customers via order_id
#}
{% macro engine_amplitude_users(tenant_slug) %}

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
