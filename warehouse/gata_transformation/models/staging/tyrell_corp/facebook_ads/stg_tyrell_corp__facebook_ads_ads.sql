-- depends_on: {{ ref('platform_sat__source_schema_history') }}
-- depends_on: {{ ref('platform_mm__facebook_ads_api_v1_ads') }}
{{ config(materialized='view', post_hook=["{{ sync_to_schema_history() }}", "{{ sync_to_master_hub('facebook_ads_api_v1_ads') }}"]) }}

WITH latest_config AS (
    SELECT tenant_slug, tenant_skey 
    FROM {{ ref('platform_sat__tenant_config_history') }}
    WHERE tenant_slug = 'tyrell_corp'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tenant_slug ORDER BY updated_at DESC) = 1
),
source_meta AS (
    SELECT 
        '4f7264cdb1738eb6525248a79e613668'::VARCHAR as source_schema_hash,
        '{"_dlt_id": "Type: VARCHAR", "_dlt_load_id": "Type: VARCHAR", "adset_id": "Type: VARCHAR", "campaign_id": "Type: VARCHAR", "creative_id": "Type: VARCHAR", "id": "Type: VARCHAR", "name": "Type: VARCHAR", "status": "Type: VARCHAR"}'::JSON as source_schema,
        'facebook_ads_api_v1_ads'::VARCHAR as master_model_ref
)

SELECT
    c.tenant_slug,
    c.tenant_skey,
    'facebook_ads'::VARCHAR as source_platform,
    m.source_schema_hash,
    m.source_schema,
    m.master_model_ref,
    t._src_table,
    -- Data Vault Payload
    to_json(t) as raw_data_payload
FROM (
    SELECT *, 'facebook_ads_ads' as _src_table
    FROM {{ ref('src_tyrell_corp_facebook_ads__ads') }}
) t
CROSS JOIN latest_config c
CROSS JOIN source_meta m