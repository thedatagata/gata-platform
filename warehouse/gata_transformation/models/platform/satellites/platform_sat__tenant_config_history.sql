{{ config(materialized='table') }}

WITH raw_manifest AS (
    SELECT tenant_slug, sources_config, updated_at
    FROM {{ ref('stg_platform_ops__tenant_manifest') }}
),

flattened_tables AS (
    SELECT
        m.tenant_slug,
        src.key as source_name,
        UNNEST(json_transform(src.value->'tables', '["JSON"]')) as table_config,
        m.updated_at
    FROM raw_manifest m,
    LATERAL (SELECT * FROM UNNEST(json_transform(m.sources_config, '["JSON"]'))) as src
)

SELECT
    tenant_slug,
    source_name,
    json_extract_string(table_config, '$.name') as table_name,
    table_config->'logic' as table_logic,
    -- Deterministic hash of the table-level logic block
    md5(json_serialize(table_config->'logic')) as logic_hash,
    updated_at
FROM flattened_tables
QUALIFY ROW_NUMBER() OVER (PARTITION BY tenant_slug, source_name, table_name, logic_hash ORDER BY updated_at DESC) = 1