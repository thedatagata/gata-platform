{{ config(materialized='table') }}

-- This table maps unique schema hashes to logical master model types
SELECT 
    source_schema_hash,
    source_table_name,
    -- Initially, we map known hashes to their platform types
    -- We use a simplified mapping logic for the baseline
    CASE 
        WHEN platform_name = 'facebook_ads' THEN 'facebook_ads_v1'
        WHEN platform_name = 'google_ads' THEN 'google_ads_v1'
        WHEN platform_name = 'instagram_ads' THEN 'facebook_ads_v1' -- Experimental mapping
        ELSE 'unknown'
    END as master_model_type,
    current_timestamp as registered_at
FROM {{ ref('platform_sat__source_schema_history') }}
WHERE source_schema_hash IS NOT NULL
