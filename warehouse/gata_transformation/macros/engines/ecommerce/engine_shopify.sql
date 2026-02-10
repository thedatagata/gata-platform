{# 
  Engine: Shopify → Orders
  Reads: int_{tenant_slug}__shopify_orders
#}
{% macro engine_shopify_orders(tenant_slug) %}
SELECT
    tenant_slug,
    source_platform,
    order_id,
    order_created_at    AS order_date,
    total_price,
    currency,
    financial_status,
    email               AS customer_email,
    customer_id,
    line_items_json
FROM {{ ref('int_' ~ tenant_slug ~ '__shopify_orders') }}
{% endmacro %}
