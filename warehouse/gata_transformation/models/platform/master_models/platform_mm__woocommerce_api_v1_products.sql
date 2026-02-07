-- Master Model for woocommerce_api_v1_products
{{ config(materialized='incremental', unique_key='hub_key') }}

SELECT
    hub_key,
    tenant_slug,
    source_platform,
    source_schema_hash,
    raw_data_payload,
    loaded_at
FROM {{ this }}
WHERE 1=0