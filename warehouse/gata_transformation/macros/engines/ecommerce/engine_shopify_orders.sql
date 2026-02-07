{% macro engine_shopify_orders(tenant_slug) %}
{#
    Engine: Shopify Orders
    Input: platform_mm__shopify_api_v1_orders
    
    Mock data fields: id (BIGINT), name, email, created_at, processed_at, updated_at,
                      total_price (VARCHAR), subtotal_price (VARCHAR), currency,
                      financial_status, customer__id, customer__email
    
    Extracts order-level metrics for ecommerce reporting.
    Links to Stripe via note_attributes when available.
#}
SELECT
    tenant_slug,
    CAST(raw_data_payload->>'id' AS BIGINT) as order_id,
    -- Robust search logic: find the value where name is 'stripe_charge_id'
    (
        SELECT attr->>'value' 
        FROM (SELECT unnest(raw_data_payload->'note_attributes') as attr) 
        WHERE attr->>'name' = 'stripe_charge_id'
        LIMIT 1
    ) as stripe_charge_id,
    raw_data_payload->>'name' as order_name,
    CAST(raw_data_payload->>'created_at' AS TIMESTAMP) as order_created_at,
    CAST(raw_data_payload->>'processed_at' AS TIMESTAMP) as order_processed_at,
    CAST(raw_data_payload->>'created_at' AS DATE) as date,
    CAST(raw_data_payload->>'total_price' AS DOUBLE) as total_price,
    CAST(raw_data_payload->>'subtotal_price' AS DOUBLE) as subtotal_price,
    raw_data_payload->>'currency' as currency,
    raw_data_payload->>'financial_status' as financial_status,
    raw_data_payload->>'email' as order_email,
    CAST(raw_data_payload->>'customer'->>'id' AS BIGINT) as customer_id,
    raw_data_payload->>'customer'->>'email' as customer_email,
    raw_data_payload
FROM {{ ref('platform_mm__shopify_api_v1_orders') }}
WHERE tenant_slug = '{{ tenant_slug }}'
{% endmacro %}
