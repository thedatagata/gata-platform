-- depends_on: {{ ref('platform_sat__source_schema_history') }}
-- depends_on: {{ ref('platform_mm__google_ads_api_v1_ad_performance') }}
{{ config(materialized='view', post_hook=["{{ sync_to_schema_history() }}", "{{ sync_to_master_hub('google_ads_api_v1_ad_performance') }}"]) }}

WITH latest_config AS (
    SELECT tenant_slug, tenant_skey 
    FROM {{ ref('platform_sat__tenant_config_history') }}
    WHERE tenant_slug = 'wayne_enterprises'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tenant_slug ORDER BY updated_at DESC) = 1
),
source_meta AS (
    SELECT 
        'b9dd59debe155ed9b2c30e6c5850da8b'::VARCHAR as source_schema_hash,
        '{"_dlt_id": "Type: VARCHAR", "_dlt_load_id": "Type: VARCHAR", "ad_group_id": "Type: VARCHAR", "ad_id": "Type: VARCHAR", "campaign_id": "Type: VARCHAR", "clicks": "Type: BIGINT", "conversions": "Type: BIGINT", "cost_micros": "Type: BIGINT", "customer_id": "Type: VARCHAR", "date": "Type: DATE", "impressions": "Type: BIGINT"}'::JSON as source_schema,
        'google_ads_api_v1_ad_performance'::VARCHAR as master_model_ref
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
    SELECT *, 'google_ads_ad_performance' as _src_table
    FROM {{ ref('src_wayne_enterprises_google_ads__ad_performance') }}
) t
CROSS JOIN latest_config c
CROSS JOIN source_meta m