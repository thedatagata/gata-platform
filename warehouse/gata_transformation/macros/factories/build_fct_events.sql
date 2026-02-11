{#
  Factory: Raw Events
  Finds the tenant's single analytics event engine and calls it.
  Discovers engine by convention: engine_{source}_events

  Usage: {{ build_fct_events('tyrell_corp') }}
#}
{% macro build_fct_events(tenant_slug) %}

{%- set tenant_config = get_tenant_config(tenant_slug) -%}
{%- set ns = namespace(engine_fn=none) -%}

{%- if tenant_config and tenant_config.get('sources') -%}
    {%- for source, config in tenant_config['sources'].items() -%}
        {%- if config.get('enabled') and ns.engine_fn is none -%}
            {%- set found = context.get('engine_' ~ source ~ '_events') -%}
            {%- if found -%}
                {%- set ns.engine_fn = found -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif -%}

{%- if ns.engine_fn is not none -%}
    {{ ns.engine_fn(tenant_slug) }}
{%- else -%}
    SELECT
        CAST(NULL AS VARCHAR)   AS tenant_slug,
        CAST(NULL AS VARCHAR)   AS source_platform,
        CAST(NULL AS VARCHAR)   AS event_name,
        CAST(NULL AS BIGINT)    AS event_timestamp,
        CAST(NULL AS VARCHAR)   AS user_pseudo_id,
        CAST(NULL AS VARCHAR)   AS user_id,
        CAST(NULL AS VARCHAR)   AS session_id,
        CAST(NULL AS VARCHAR)   AS order_id,
        CAST(NULL AS DOUBLE)    AS order_total,
        CAST(NULL AS VARCHAR)   AS traffic_source,
        CAST(NULL AS VARCHAR)   AS traffic_medium,
        CAST(NULL AS VARCHAR)   AS traffic_campaign,
        CAST(NULL AS VARCHAR)   AS geo_country,
        CAST(NULL AS VARCHAR)   AS device_category
    WHERE 1 = 0
{%- endif -%}

{% endmacro %}
