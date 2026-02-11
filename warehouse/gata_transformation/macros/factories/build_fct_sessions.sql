{#
  Factory: Sessions
  Finds the tenant's single analytics session engine and calls it.
  Discovers engine by convention: engine_{source}_sessions

  Usage: {{ build_fct_sessions('tyrell_corp') }}
#}
{% macro build_fct_sessions(tenant_slug) %}

{%- set tenant_config = get_tenant_config(tenant_slug) -%}
{%- set ns = namespace(engine_fn=none, events=[]) -%}

{%- if tenant_config and tenant_config.get('sources') -%}
    {%- for source, config in tenant_config['sources'].items() -%}
        {%- if config.get('enabled') and ns.engine_fn is none -%}
            {%- set found = context.get('engine_' ~ source ~ '_sessions') -%}
            {%- if found -%}
                {%- set ns.engine_fn = found -%}
                {%- set ns.events = config.get('logic', {}).get('conversion_events', []) -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif -%}

{%- if ns.engine_fn is not none -%}
    {{ ns.engine_fn(tenant_slug, ns.events) }}
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
