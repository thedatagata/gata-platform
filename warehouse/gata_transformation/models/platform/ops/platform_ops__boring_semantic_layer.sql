{{ config(materialized='table') }}

-- Aggregates the latest manifest for every model across the platform
SELECT 
    tenant_slug,
    platform_name,
    source_table_name,
    boring_semantic_manifest as semantic_manifest
FROM {{ ref('platform_sat__source_schema_history') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY tenant_slug, source_table_name 
    ORDER BY updated_at DESC
) = 1
