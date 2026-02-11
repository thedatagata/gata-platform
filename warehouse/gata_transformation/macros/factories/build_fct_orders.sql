{#
  Factory: Orders
  Finds the tenant's single ecommerce engine and calls it.
  Discovers engine by convention: engine_{source}_orders

  Usage: {{ build_fct_orders('tyrell_corp') }}
#}
{% macro build_fct_orders(tenant_slug) %}

{%- set tenant_config = get_tenant_config(tenant_slug) -%}
{%- set ns = namespace(engine_fn=none) -%}

{%- if tenant_config and tenant_config.get('sources') -%}
    {%- for source, config in tenant_config['sources'].items() -%}
        {%- if config.get('enabled') and ns.engine_fn is none -%}
            {%- set found = context.get('engine_' ~ source ~ '_orders') -%}
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
        CAST(NULL AS BIGINT)    AS order_id,
        CAST(NULL AS TIMESTAMP) AS order_date,
        CAST(NULL AS DOUBLE)    AS total_price,
        CAST(NULL AS VARCHAR)   AS currency,
        CAST(NULL AS VARCHAR)   AS financial_status,
        CAST(NULL AS VARCHAR)   AS customer_email,
        CAST(NULL AS VARCHAR)   AS customer_id,
        CAST(NULL AS JSON)      AS line_items_json
    WHERE 1 = 0
{%- endif -%}

{% endmacro %}
