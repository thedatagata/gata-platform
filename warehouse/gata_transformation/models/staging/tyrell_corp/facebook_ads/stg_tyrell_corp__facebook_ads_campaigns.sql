-- depends_on: {{ ref('platform_sat__source_schema_history') }}
-- depends_on: {{ ref('platform_mm__facebook_ads_api_v1_campaigns') }}
{{ config(materialized='view', post_hook=["{{ sync_to_schema_history() }}", "{{ sync_to_master_hub('facebook_ads_api_v1_campaigns') }}"]) }}

WITH source_meta AS (
    SELECT 
        '1e802e2016a1dab0420200922898b19e'::VARCHAR as source_schema_hash,
        '{"_dlt_id": "Type: VARCHAR", "_dlt_load_id": "Type: VARCHAR", "id": "Type: VARCHAR", "name": "Type: VARCHAR", "objective": "Type: VARCHAR", "status": "Type: VARCHAR"}'::JSON as source_schema,
        'facebook_ads_api_v1_campaigns'::VARCHAR as master_model_ref
)

SELECT
    'tyrell_corp'::VARCHAR,
    {{ generate_tenant_key("'tyrell_corp'") }},
    'facebook_ads'::VARCHAR as source_platform,
    m.source_schema_hash,
    m.source_schema,
    m.master_model_ref,
    t._src_table,
    -- Data Vault Payload
    to_json(t) as raw_data_payload
FROM (
    SELECT *, 'facebook_ads_campaigns' as _src_table
    FROM {{ ref('src_tyrell_corp_facebook_ads__campaigns') }}
) t
CROSS JOIN source_meta m