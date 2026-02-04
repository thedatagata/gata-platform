-- depends_on: {{ ref('platform_sat__source_schema_history') }}
-- depends_on: {{ ref('platform_mm__shopify_api_v1_orders') }}
{{ config(materialized='view', post_hook=["{{ sync_to_schema_history() }}", "{{ sync_to_master_hub('shopify_api_v1_orders') }}"]) }}

WITH latest_config AS (
    SELECT tenant_slug, tenant_skey 
    FROM {{ ref('platform_sat__tenant_config_history') }}
    WHERE tenant_slug = 'tyrell_corp'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tenant_slug ORDER BY updated_at DESC) = 1
),
source_meta AS (
    SELECT 
        'fc3c952b1fcbba2ea269f75429c538ff'::VARCHAR as source_schema_hash,
        '{"_dlt_id": "Type: VARCHAR", "_dlt_load_id": "Type: VARCHAR", "created_at": "Type: TIMESTAMP WITH TIME ZONE", "currency": "Type: VARCHAR", "customer__email": "Type: VARCHAR", "customer__id": "Type: BIGINT", "email": "Type: VARCHAR", "financial_status": "Type: VARCHAR", "id": "Type: BIGINT", "name": "Type: VARCHAR", "processed_at": "Type: TIMESTAMP WITH TIME ZONE", "subtotal_price": "Type: VARCHAR", "total_price": "Type: VARCHAR", "updated_at": "Type: TIMESTAMP WITH TIME ZONE"}'::JSON as source_schema,
        'shopify_api_v1_orders'::VARCHAR as master_model_ref
)

SELECT
    c.tenant_slug,
    c.tenant_skey,
    'shopify'::VARCHAR as source_platform,
    m.source_schema_hash,
    m.source_schema,
    m.master_model_ref,
    t._src_table,
    -- Data Vault Payload
    to_json(t) as raw_data_payload
FROM (
    SELECT *, 'shopify_orders' as _src_table
    FROM {{ ref('src_tyrell_corp_shopify__orders') }}
) t
CROSS JOIN latest_config c
CROSS JOIN source_meta m