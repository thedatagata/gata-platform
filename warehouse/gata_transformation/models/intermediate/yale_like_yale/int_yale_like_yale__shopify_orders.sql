{{ config(materialized='table') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    CAST(raw_data_payload->>'$.id' AS BIGINT) AS order_id,
    raw_data_payload->>'$.name' AS order_name,
    raw_data_payload->>'$.email' AS email,
    CAST(raw_data_payload->>'$.total_price' AS DOUBLE) AS total_price,
    raw_data_payload->>'$.currency' AS currency,
    raw_data_payload->>'$.financial_status' AS financial_status,
    raw_data_payload->>'$.customer_email' AS customer_email,
    raw_data_payload->>'$.customer_id' AS customer_id,
    CAST(raw_data_payload->>'$.created_at' AS TIMESTAMP) AS order_created_at,
    raw_data_payload->'$.line_items' AS line_items_json,

    raw_data_payload

FROM {{ ref('platform_mm__shopify_api_v1_orders') }}
WHERE tenant_slug = 'yale_like_yale'
  AND source_platform = 'shopify'