{#
  Factory: User Dimension (Cross-Platform Identity Resolution)
  Finds the tenant's analytics user engine and calls it.
  Discovers engine by convention: engine_{source}_users

  Usage: {{ build_dim_users('tyrell_corp') }}
#}
{% macro build_dim_users(tenant_slug) %}

{%- set tenant_config = get_tenant_config(tenant_slug) -%}
{%- set ns = namespace(engine_fn=none) -%}

{%- if tenant_config and tenant_config.get('sources') -%}
    {%- for source, config in tenant_config['sources'].items() -%}
        {%- if config.get('enabled') and ns.engine_fn is none -%}
            {%- set found = context.get('engine_' ~ source ~ '_users') -%}
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
        CAST(NULL AS VARCHAR)   AS user_pseudo_id,
        CAST(NULL AS VARCHAR)   AS user_id,
        CAST(NULL AS VARCHAR)   AS customer_email,
        CAST(NULL AS VARCHAR)   AS customer_id,
        CAST(NULL AS BOOLEAN)   AS is_customer,
        CAST(NULL AS BIGINT)    AS first_seen_at,
        CAST(NULL AS BIGINT)    AS last_seen_at,
        CAST(NULL AS BIGINT)    AS total_events,
        CAST(NULL AS BIGINT)    AS total_sessions,
        CAST(NULL AS VARCHAR)   AS first_geo_country,
        CAST(NULL AS VARCHAR)   AS first_device_category
    WHERE 1 = 0
{%- endif -%}

{% endmacro %}
