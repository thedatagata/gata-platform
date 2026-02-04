-- depends_on: {{ ref('platform_sat__source_schema_history') }}
-- depends_on: {{ ref('platform_mm__google_ads_api_v1_customers') }}
{{ config(materialized='view', post_hook=["{{ sync_to_schema_history() }}", "{{ sync_to_master_hub('google_ads_api_v1_customers') }}"]) }}

WITH latest_config AS (
    SELECT tenant_slug, tenant_skey 
    FROM {{ ref('platform_sat__tenant_config_history') }}
    WHERE tenant_slug = 'wayne_enterprises'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tenant_slug ORDER BY updated_at DESC) = 1
),
source_meta AS (
    SELECT 
        '775dc6ce7b8cef849e1cf4d56d7c2eef'::VARCHAR as source_schema_hash,
        '{"_dlt_id": "Type: VARCHAR", "_dlt_load_id": "Type: VARCHAR", "currency_code": "Type: VARCHAR", "descriptive_name": "Type: VARCHAR", "id": "Type: VARCHAR", "resource_name": "Type: VARCHAR", "time_zone": "Type: VARCHAR"}'::JSON as source_schema,
        'google_ads_api_v1_customers'::VARCHAR as master_model_ref
)

SELECT
    c.tenant_slug,
    c.tenant_skey,
    'google_ads'::VARCHAR as source_platform,
    m.source_schema_hash,
    m.source_schema,
    m.master_model_ref,
    t._src_table,
    -- Data Vault Payload
    to_json(t) as raw_data_payload
FROM (
    SELECT *, 'google_ads_customers' as _src_table
    FROM {{ ref('src_wayne_enterprises_google_ads__customers') }}
) t
CROSS JOIN latest_config c
CROSS JOIN source_meta m