-- Intermediate: Tyrell Corp Shopify Orders
{{ config(materialized='view') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    CAST(raw_data_payload->>'$.id' AS BIGINT)                    AS order_id,
    raw_data_payload->>'$.name'                                   AS order_name,
    raw_data_payload->>'$.email'                                  AS email,
    CAST(raw_data_payload->>'$.total_price' AS DOUBLE)           AS total_price,
    raw_data_payload->>'$.currency'                               AS currency,
    raw_data_payload->>'$.financial_status'                       AS financial_status,
    raw_data_payload->>'$.status'                                 AS order_status,
    CAST(raw_data_payload->>'$.created_at' AS TIMESTAMP)         AS order_created_at,

    -- Nested objects kept as JSON for downstream parsing
    raw_data_payload->'$.customer'                                AS customer_json,
    raw_data_payload->'$.line_items'                              AS line_items_json,

    raw_data_payload

FROM {{ ref('platform_mm__shopify_api_v1_orders') }}
WHERE tenant_slug = 'tyrell_corp'
  AND source_platform = 'shopify'
