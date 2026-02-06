-- depends_on: {{ ref('platform_sat__source_schema_history') }}
-- depends_on: {{ ref('platform_mm__google_analytics_api_v1_events') }}
{{ config(materialized='view', post_hook=["{{ sync_to_schema_history() }}", "{{ sync_to_master_hub('google_analytics_api_v1_events') }}"]) }}

WITH source_meta AS (
    SELECT 
        'c80d3a67870bdde5d097d58eca1223bf'::VARCHAR as source_schema_hash,
        '{"_dlt_id": "Type: VARCHAR", "_dlt_load_id": "Type: VARCHAR", "ecommerce__currency": "Type: VARCHAR", "ecommerce__transaction_id": "Type: VARCHAR", "ecommerce__value": "Type: DOUBLE", "event_date": "Type: VARCHAR", "event_name": "Type: VARCHAR", "event_timestamp": "Type: BIGINT", "geo__city": "Type: VARCHAR", "geo__country": "Type: VARCHAR", "traffic_source__campaign": "Type: VARCHAR", "traffic_source__medium": "Type: VARCHAR", "traffic_source__source": "Type: VARCHAR", "user_pseudo_id": "Type: VARCHAR"}'::JSON as source_schema,
        'google_analytics_api_v1_events'::VARCHAR as master_model_ref
)

SELECT
    'tyrell_corp'::VARCHAR,
    {{ generate_tenant_key("'tyrell_corp'") }},
    'google_analytics'::VARCHAR as source_platform,
    m.source_schema_hash,
    m.source_schema,
    m.master_model_ref,
    t._src_table,
    -- Data Vault Payload
    to_json(t) as raw_data_payload
FROM (
    SELECT *, 'google_analytics_events' as _src_table
    FROM {{ ref('src_tyrell_corp_google_analytics__events') }}
) t
CROSS JOIN source_meta m