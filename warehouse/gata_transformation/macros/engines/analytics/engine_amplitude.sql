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

    {% if conversion_events | length > 0 %}
    MAX(CASE WHEN event_name IN ({% for evt in conversion_events %}'{{ evt }}'{% if not loop.last %}, {% endif %}{% endfor %}) THEN TRUE ELSE FALSE END) AS is_conversion_session,
    {% else %}
    FALSE AS is_conversion_session,
    {% endif %}

    CAST(0 AS DOUBLE) AS session_revenue,
    CAST(NULL AS VARCHAR) AS transaction_id

FROM {{ ref('int_' ~ tenant_slug ~ '__amplitude_events') }}
WHERE session_id IS NOT NULL
GROUP BY tenant_slug, source_platform, session_id, user_pseudo_id, user_id

{% endmacro %}
