{# 
  Engine: WooCommerce → Orders
  Reads: int_{tenant_slug}__woocommerce_orders
#}
{% macro engine_woocommerce_orders(tenant_slug) %}
SELECT
    tenant_slug,
    source_platform,
    order_id,
    order_created_at    AS order_date,
    total_price,
    currency,
    order_status        AS financial_status,
    customer_email,
    CAST(customer_id AS VARCHAR) AS customer_id,
    line_items_json
FROM {{ ref('int_' ~ tenant_slug ~ '__woocommerce_orders') }}
{% endmacro %}
