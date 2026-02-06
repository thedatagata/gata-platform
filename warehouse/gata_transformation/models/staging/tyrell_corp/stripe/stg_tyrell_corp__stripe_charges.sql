-- depends_on: {{ ref('platform_sat__source_schema_history') }}
-- depends_on: {{ ref('platform_mm__stripe_api_v1_charges') }}
{{ config(materialized='view', post_hook=["{{ sync_to_schema_history() }}", "{{ sync_to_master_hub('stripe_api_v1_charges') }}"]) }}

WITH source_meta AS (
    SELECT 
        '14569fc468cc217e53315dd992048fb2'::VARCHAR as source_schema_hash,
        '{"_dlt_id": "Type: VARCHAR", "_dlt_load_id": "Type: VARCHAR", "amount": "Type: BIGINT", "amount_captured": "Type: BIGINT", "amount_refunded": "Type: BIGINT", "created": "Type: TIMESTAMP WITH TIME ZONE", "currency": "Type: VARCHAR", "id": "Type: VARCHAR", "paid": "Type: BOOLEAN", "payment_method_details__card__brand": "Type: VARCHAR", "payment_method_details__card__last4": "Type: VARCHAR", "payment_method_details__type": "Type: VARCHAR", "refunded": "Type: BOOLEAN", "status": "Type: VARCHAR"}'::JSON as source_schema,
        'stripe_api_v1_charges'::VARCHAR as master_model_ref
)

SELECT
    'tyrell_corp'::VARCHAR,
    {{ generate_tenant_key("'tyrell_corp'") }},
    'stripe'::VARCHAR as source_platform,
    m.source_schema_hash,
    m.source_schema,
    m.master_model_ref,
    t._src_table,
    -- Data Vault Payload
    to_json(t) as raw_data_payload
FROM (
    SELECT *, 'stripe_charges' as _src_table
    FROM {{ ref('src_tyrell_corp_stripe__charges') }}
) t
CROSS JOIN source_meta m