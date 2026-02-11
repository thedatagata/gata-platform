{#
  Factory: Orders
  Unions all ecommerce engines for a tenant's enabled ecommerce sources.
  
  Usage: {{ build_fct_orders('tyrell_corp', ['shopify']) }}
  
  Source → Engine mapping:
    shopify      → engine_shopify_orders
    bigcommerce  → engine_bigcommerce_orders
    woocommerce  → engine_woocommerce_orders
#}
{% macro build_fct_orders(tenant_slug, ecommerce_sources) %}

{%- set engine_map = {
    'shopify':      'engine_shopify_orders',
    'bigcommerce':  'engine_bigcommerce_orders',
    'woocommerce':  'engine_woocommerce_orders'
} -%}

{%- set ns = namespace(first=true) -%}

{%- for source in ecommerce_sources -%}
    {%- if source in engine_map -%}
        {%- if not ns.first %} UNION ALL {% endif -%}
        {{ context[engine_map[source]](tenant_slug) }}
        {%- set ns.first = false -%}
    {%- endif -%}
{%- endfor -%}

{%- if ns.first -%}
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
