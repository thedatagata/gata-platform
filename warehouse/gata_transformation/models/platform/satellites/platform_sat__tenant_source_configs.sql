{{ config(materialized='table', tags=["onboarding", "platform"]) }}

SELECT
    tenant_skey,
    tenant_slug,
    source_name as platform_name,
    table_name,
    table_logic,
    logic_hash,
    updated_at
FROM {{ ref('platform_sat__tenant_config_history') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY tenant_slug, source_name, table_name ORDER BY updated_at DESC) = 1