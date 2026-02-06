-- depends_on: {{ ref('platform_sat__source_schema_history') }}
-- depends_on: {{ ref('platform_mm__shopify_api_v1_products') }}
{{ config(materialized='view', post_hook=["{{ sync_to_schema_history() }}", "{{ sync_to_master_hub('shopify_api_v1_products') }}"]) }}

WITH source_meta AS (
    SELECT 
        '01bd2d79f3f1805a725c8b12ecba6e96'::VARCHAR as source_schema_hash,
        '{"_dlt_id": "Type: VARCHAR", "_dlt_load_id": "Type: VARCHAR", "created_at": "Type: TIMESTAMP WITH TIME ZONE", "id": "Type: BIGINT", "product_type": "Type: VARCHAR", "status": "Type: VARCHAR", "title": "Type: VARCHAR"}'::JSON as source_schema,
        'shopify_api_v1_products'::VARCHAR as master_model_ref
)

SELECT
    'wayne_enterprises'::VARCHAR,
    {{ generate_tenant_key("'wayne_enterprises'") }},
    'shopify'::VARCHAR as source_platform,
    m.source_schema_hash,
    m.source_schema,
    m.master_model_ref,
    t._src_table,
    -- Data Vault Payload
    to_json(t) as raw_data_payload
FROM (
    SELECT *, 'shopify_products' as _src_table
    FROM {{ ref('src_wayne_enterprises_shopify__products') }}
) t
CROSS JOIN source_meta m