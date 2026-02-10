{{ config(materialized='table') }}

WITH history AS (
    SELECT
        tenant_slug,
        tenant_skey,
        source_name,
        table_name,
        logic_hash,
        updated_at
    FROM {{ ref('platform_sat__tenant_config_history') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tenant_slug, source_name, table_name ORDER BY updated_at DESC) = 1
)

SELECT
    h.tenant_slug as client_slug,
    h.source_name as platform_name,
    h.table_name,
    {{ generate_tenant_key('h.tenant_slug', 'h.source_name', 'h.updated_at', 'h.logic_hash') }} as generated_hash,
    h.tenant_skey,
    h.updated_at as logic_version_at,
    h.logic_hash as logic_config_hash,
    now() as logged_at
FROM history h