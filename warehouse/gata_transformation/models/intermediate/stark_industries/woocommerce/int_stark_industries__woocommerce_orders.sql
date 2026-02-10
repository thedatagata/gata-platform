-- Intermediate: Stark Industries WooCommerce Orders
{{ config(materialized='view') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    CAST(raw_data_payload->>'$.id' AS BIGINT)                    AS order_id,
    raw_data_payload->>'$.number'                                 AS order_number,
    raw_data_payload->>'$.status'                                 AS order_status,
    CAST(raw_data_payload->>'$.total_price' AS DOUBLE)           AS total_price,
    raw_data_payload->>'$.currency'                               AS currency,
    CAST(raw_data_payload->>'$.customer_id' AS BIGINT)           AS customer_id,
    raw_data_payload->>'$.billing_email'                          AS customer_email,
    CAST(raw_data_payload->>'$.created_at' AS TIMESTAMP)         AS order_created_at,

    -- Nested JSON preserved for downstream
    raw_data_payload->'$.line_items'                              AS line_items_json,

    raw_data_payload

FROM {{ ref('platform_mm__woocommerce_api_v1_orders') }}
WHERE tenant_slug = 'stark_industries'
  AND source_platform = 'woocommerce'
