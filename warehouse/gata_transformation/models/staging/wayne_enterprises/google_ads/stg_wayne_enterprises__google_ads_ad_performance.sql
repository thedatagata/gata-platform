-- depends_on: {{ ref('platform_sat__source_schema_history') }}
-- depends_on: {{ ref('platform_mm__google_ads_api_v1_ad_performance') }}
{{ config(materialized='view', post_hook=["{{ sync_to_schema_history() }}", "{{ sync_to_master_hub('google_ads_api_v1_ad_performance') }}"]) }}

WITH source_meta AS (
    SELECT 
        'b9dd59debe155ed9b2c30e6c5850da8b'::VARCHAR as source_schema_hash,
        '{"_dlt_id": "Type: VARCHAR", "_dlt_load_id": "Type: VARCHAR", "ad_group_id": "Type: VARCHAR", "ad_id": "Type: VARCHAR", "campaign_id": "Type: VARCHAR", "clicks": "Type: BIGINT", "conversions": "Type: BIGINT", "cost_micros": "Type: BIGINT", "customer_id": "Type: VARCHAR", "date": "Type: DATE", "impressions": "Type: BIGINT"}'::JSON as source_schema,
        'google_ads_api_v1_ad_performance'::VARCHAR as master_model_ref
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
    SELECT *, 'google_ads_ad_performance' as _src_table
    FROM {{ ref('src_wayne_enterprises_google_ads__ad_performance') }}
) t
CROSS JOIN source_meta m