{# 
  Engine: BigCommerce → Orders
  Reads: int_{tenant_slug}__bigcommerce_orders
#}
{% macro engine_bigcommerce_orders(tenant_slug) %}
SELECT
    tenant_slug,
    source_platform,
    order_id,
    order_created_at    AS order_date,
    total_price,
    currency,
    order_status        AS financial_status,
    CAST(NULL AS VARCHAR) AS customer_email,
    CAST(customer_id AS VARCHAR) AS customer_id,
    CAST(NULL AS JSON)  AS line_items_json
FROM {{ ref('int_' ~ tenant_slug ~ '__bigcommerce_orders') }}
{% endmacro %}
