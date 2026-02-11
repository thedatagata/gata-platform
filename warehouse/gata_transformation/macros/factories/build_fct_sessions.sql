{#
  Factory: Sessions
  Calls the appropriate analytics engine for a tenant's analytics source.
  
  Usage: {{ build_fct_sessions('tyrell_corp', 'google_analytics', ['purchase']) }}
  
  Source → Engine mapping:
    google_analytics → engine_google_analytics_sessions
    amplitude        → engine_amplitude_sessions
    mixpanel         → engine_mixpanel_sessions
#}
{% macro build_fct_sessions(tenant_slug, analytics_source, conversion_events=[]) %}

{%- if analytics_source == 'google_analytics' -%}
    {{ engine_google_analytics_sessions(tenant_slug, conversion_events) }}
{%- elif analytics_source == 'amplitude' -%}
    {{ engine_amplitude_sessions(tenant_slug, conversion_events) }}
{%- elif analytics_source == 'mixpanel' -%}
    {{ engine_mixpanel_sessions(tenant_slug, conversion_events) }}
{%- else -%}
    SELECT
        CAST(NULL AS VARCHAR)   AS tenant_slug,
        CAST(NULL AS VARCHAR)   AS source_platform,
        CAST(NULL AS VARCHAR)   AS session_id,
        CAST(NULL AS VARCHAR)   AS user_pseudo_id,
        CAST(NULL AS VARCHAR)   AS user_id,
        CAST(NULL AS BIGINT)    AS session_start_ts,
        CAST(NULL AS BIGINT)    AS session_end_ts,
        CAST(NULL AS BIGINT)    AS session_duration_seconds,
        CAST(NULL AS BIGINT)    AS events_in_session,
        CAST(NULL AS VARCHAR)   AS traffic_source,
        CAST(NULL AS VARCHAR)   AS traffic_medium,
        CAST(NULL AS VARCHAR)   AS traffic_campaign,
        CAST(NULL AS VARCHAR)   AS geo_country,
        CAST(NULL AS VARCHAR)   AS device_category,
        CAST(NULL AS BOOLEAN)   AS is_conversion_session,
        CAST(NULL AS DOUBLE)    AS session_revenue,
        CAST(NULL AS VARCHAR)   AS transaction_id
    WHERE 1 = 0
{%- endif -%}

{% endmacro %}
