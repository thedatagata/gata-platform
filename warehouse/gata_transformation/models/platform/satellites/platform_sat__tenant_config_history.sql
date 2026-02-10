{{ config(materialized='table') }}

WITH sources_unpacked AS (
    SELECT
        m.tenant_slug,
        {{ generate_tenant_key("m.tenant_slug") }} as tenant_skey,
        src.source_name,
        json_extract(m.sources_config, '$.' || src.source_name || '.tables') as tables_json,
        m.loaded_at as updated_at
    FROM {{ ref('stg_platform_ops__tenant_manifest') }} m
    CROSS JOIN LATERAL (SELECT unnest(json_keys(m.sources_config)) as source_name) src
),

flattened AS (
    SELECT
        s.tenant_slug,
        s.tenant_skey,
        s.source_name,
        t.table_cfg,
        s.updated_at
    FROM sources_unpacked s
    CROSS JOIN LATERAL (SELECT unnest(from_json(s.tables_json, '["JSON"]')) as table_cfg) t
    WHERE s.tables_json IS NOT NULL
)

SELECT
    tenant_slug,
    tenant_skey,
    source_name,
    json_extract_string(table_cfg, '$.name') as table_name,
    table_cfg->'$.logic' as table_logic,
    md5(CAST(table_cfg->'$.logic' AS VARCHAR)) as logic_hash,
    updated_at
FROM flattened
QUALIFY ROW_NUMBER() OVER (PARTITION BY tenant_slug, source_name, json_extract_string(table_cfg, '$.name'), md5(CAST(table_cfg->'$.logic' AS VARCHAR)) ORDER BY updated_at DESC) = 1