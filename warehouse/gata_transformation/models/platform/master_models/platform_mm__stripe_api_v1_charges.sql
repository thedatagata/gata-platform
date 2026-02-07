{{ config(materialized='table') }}

SELECT 
    CAST(NULL AS VARCHAR) as tenant_slug,
    CAST(NULL AS VARCHAR) as tenant_skey,
    CAST(NULL AS VARCHAR) as source_platform,
    CAST(NULL AS VARCHAR) as source_schema_hash,
    CAST(NULL AS JSON) as source_schema,
    CAST(NULL AS JSON) as raw_data_payload,
    CAST(NULL AS TIMESTAMP) as loaded_at
WHERE 1=0
