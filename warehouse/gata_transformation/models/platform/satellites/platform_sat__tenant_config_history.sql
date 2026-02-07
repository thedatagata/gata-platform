{{ config(materialized='table') }}

WITH flattened AS (
    SELECT 
        m.tenant_slug, src.key as source_name, 
        UNNEST(json_transform(src.value->'tables', '["JSON"]')) as table_cfg, 
        m.updated_at
    FROM {{ ref('stg_platform_ops__tenant_manifest') }} m,
    LATERAL (SELECT * FROM UNNEST(json_transform(m.sources_config, '["JSON"]'))) as src
)
SELECT 
    tenant_slug, source_name, 
    json_extract_string(table_cfg, '$.name') as table_name,
    table_cfg->'logic' as table_logic,
    md5(json_serialize(table_cfg->'logic')) as logic_hash,
    updated_at
FROM flattened
QUALIFY ROW_NUMBER() OVER (PARTITION BY tenant_slug, source_name, table_name, logic_hash ORDER BY updated_at DESC) = 1