-- Intermediate: Tyrell Corp Shopify Products
{{ config(materialized='view') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    CAST(raw_data_payload->>'$.id' AS BIGINT)          AS product_id,
    raw_data_payload->>'$.title'                        AS product_name,
    raw_data_payload->>'$.product_type'                 AS product_type,
    raw_data_payload->>'$.status'                       AS status,
    CAST(raw_data_payload->>'$.price' AS DOUBLE)       AS price,
    CAST(raw_data_payload->>'$.created_at' AS TIMESTAMP) AS created_at,

    raw_data_payload

FROM {{ ref('platform_mm__shopify_api_v1_products') }}
WHERE tenant_slug = 'tyrell_corp'
  AND source_platform = 'shopify'
