{{ config(materialized='table') }}

-- Sink Table Pattern: Empty shell that gets populated by post-hooks
-- This removes compile-time dependencies on staging models

SELECT
    CAST(NULL AS VARCHAR) as tenant_slug,
    CAST(NULL AS VARCHAR) as platform_name,
    CAST(NULL AS VARCHAR) as source_table_name,
    CAST(NULL AS VARCHAR) as source_schema_hash,
    CAST(NULL AS JSON) as source_schema,
    CAST(NULL AS VARCHAR) as source_schema_skey,
    CAST(NULL AS TIMESTAMP) as first_seen_at,
    CAST(NULL AS TIMESTAMP) as updated_at
WHERE 1=0