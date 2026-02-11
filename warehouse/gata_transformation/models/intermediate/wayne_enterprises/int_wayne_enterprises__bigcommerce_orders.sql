{{ config(materialized='table') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    CAST(raw_data_payload->>'$.id' AS BIGINT) AS order_id,
    raw_data_payload->>'$.status' AS order_status,
    CAST(raw_data_payload->>'$.total_price' AS DOUBLE) AS total_price,
    raw_data_payload->>'$.currency' AS currency,
    CAST(raw_data_payload->>'$.customer_id' AS BIGINT) AS customer_id,
    raw_data_payload->>'$.billing_email' AS billing_email,
    CAST(raw_data_payload->>'$.created_at' AS TIMESTAMP) AS order_created_at,

    raw_data_payload

FROM {{ ref('platform_mm__bigcommerce_api_v1_orders') }}
WHERE tenant_slug = 'wayne_enterprises'
  AND source_platform = 'bigcommerce'