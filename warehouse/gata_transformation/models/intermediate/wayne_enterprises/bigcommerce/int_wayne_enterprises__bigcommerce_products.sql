-- Intermediate: Wayne Enterprises BigCommerce Products
{{ config(materialized='view') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    CAST(raw_data_payload->>'$.id' AS BIGINT)          AS product_id,
    raw_data_payload->>'$.name'                         AS product_name,
    raw_data_payload->>'$.sku'                          AS sku,
    CAST(raw_data_payload->>'$.price' AS DOUBLE)       AS price,
    raw_data_payload->>'$.availability'                 AS availability,

    raw_data_payload

FROM {{ ref('platform_mm__bigcommerce_api_v1_products') }}
WHERE tenant_slug = 'wayne_enterprises'
  AND source_platform = 'bigcommerce'
