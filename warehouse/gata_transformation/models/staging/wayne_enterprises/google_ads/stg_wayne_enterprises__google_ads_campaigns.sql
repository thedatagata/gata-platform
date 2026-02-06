-- depends_on: {{ ref('platform_sat__source_schema_history') }}
-- depends_on: {{ ref('platform_mm__google_ads_api_v1_campaigns') }}
{{ config(materialized='view', post_hook=["{{ sync_to_schema_history() }}", "{{ sync_to_master_hub('google_ads_api_v1_campaigns') }}"]) }}

WITH source_meta AS (
    SELECT 
        'd5a1617ba0c9d66d97ce684612ddf99e'::VARCHAR as source_schema_hash,
        '{"_dlt_id": "Type: VARCHAR", "_dlt_load_id": "Type: VARCHAR", "advertising_channel_type": "Type: VARCHAR", "id": "Type: VARCHAR", "name": "Type: VARCHAR", "resource_name": "Type: VARCHAR", "status": "Type: VARCHAR"}'::JSON as source_schema,
        'google_ads_api_v1_campaigns'::VARCHAR as master_model_ref
)

SELECT
    'wayne_enterprises'::VARCHAR,
    {{ generate_tenant_key("'wayne_enterprises'") }},
    'google_ads'::VARCHAR as source_platform,
    m.source_schema_hash,
    m.source_schema,
    m.master_model_ref,
    t._src_table,
    -- Data Vault Payload
    to_json(t) as raw_data_payload
FROM (
    SELECT *, 'google_ads_campaigns' as _src_table
    FROM {{ ref('src_wayne_enterprises_google_ads__campaigns') }}
) t
CROSS JOIN source_meta m