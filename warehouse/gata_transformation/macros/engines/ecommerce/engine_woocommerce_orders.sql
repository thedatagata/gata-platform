{% macro engine_woocommerce_orders(tenant_slug) %}
{#
    Engine: WooCommerce Orders
    Input: platform_mm__woocommerce_api_v1_orders
    
    Fields: id, number, status, currency, total, subtotal, total_tax,
            payment_method, date_created_gmt, billing_email, 
            billing_first_name, billing_last_name, customer_id, meta_data
    
    Extracts order metrics. Links to Stripe via meta_data (JSON string) -> _stripe_charge_id.
#}
SELECT
    tenant_slug,
    CAST(raw_data_payload->>'id' AS BIGINT) as order_id,
    raw_data_payload->>'number' as order_name,
    CAST(raw_data_payload->>'date_created_gmt' AS TIMESTAMP) as order_created_at,
    CAST(raw_data_payload->>'date_created_gmt' AS DATE) as date,
    CAST(raw_data_payload->>'total' AS DOUBLE) as total_price,
    CAST(raw_data_payload->>'subtotal' AS DOUBLE) as subtotal_price,
    raw_data_payload->>'currency' as currency,
    raw_data_payload->>'status' as financial_status,
    raw_data_payload->>'billing_email' as order_email,
    CAST(raw_data_payload->>'customer_id' AS BIGINT) as customer_id,
    raw_data_payload->>'billing_email' as customer_email,
    -- Extract Stripe Charge ID from meta_data JSON string
    -- meta_data structure: [{"key": "_stripe_charge_id", "value": "..."}]
    (
        SELECT value 
        FROM (
            SELECT json_extract_string(element, '$.key') as key, 
                   json_extract_string(element, '$.value') as value
            FROM (
                SELECT unnest(json_transform(raw_data_payload->>'meta_data', 'JSON[]')) as element
            )
        ) 
        WHERE key = '_stripe_charge_id' 
        LIMIT 1
    ) as stripe_charge_id,
    
    raw_data_payload
FROM {{ ref('platform_mm__woocommerce_api_v1_orders') }}
WHERE tenant_slug = '{{ tenant_slug }}'
{% endmacro %}
