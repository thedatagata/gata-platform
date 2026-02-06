-- depends_on: {{ ref('platform_sat__source_schema_history') }}
-- depends_on: {{ ref('platform_mm__google_ads_api_v1_ad_groups') }}
{{ config(materialized='view', post_hook=["{{ sync_to_schema_history() }}", "{{ sync_to_master_hub('google_ads_api_v1_ad_groups') }}"]) }}

WITH source_meta AS (
    SELECT 
        'a9f1827801676800a1171e9359253645'::VARCHAR as source_schema_hash,
        '{"_dlt_id": "Type: VARCHAR", "_dlt_load_id": "Type: VARCHAR", "campaign_id": "Type: VARCHAR", "id": "Type: VARCHAR", "name": "Type: VARCHAR", "resource_name": "Type: VARCHAR", "status": "Type: VARCHAR", "type": "Type: VARCHAR"}'::JSON as source_schema,
        'google_ads_api_v1_ad_groups'::VARCHAR as master_model_ref
)

SELECT
    'tyrell_corp'::VARCHAR,
    {{ generate_tenant_key("'tyrell_corp'") }},
    'google_ads'::VARCHAR as source_platform,
    m.source_schema_hash,
    m.source_schema,
    m.master_model_ref,
    t._src_table,
    -- Data Vault Payload
    to_json(t) as raw_data_payload
FROM (
    SELECT *, 'google_ads_ad_groups' as _src_table
    FROM {{ ref('src_tyrell_corp_google_ads__ad_groups') }}
) t
CROSS JOIN source_meta m