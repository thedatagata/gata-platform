
{{ config(materialized='table') }}

WITH latest_config AS (
    SELECT *
    FROM {{ ref('platform_sat__tenant_config_history') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tenant_slug ORDER BY updated_at DESC) = 1
),

registry AS (
    SELECT 
        r.client_slug,
        r.source_schema,
        r.source_table,
        r.created_at,
        r.integrated_flag,
        r.dbt_freshness_status,
        r.candidate_priority_score,
        r.tenant_status,
        c.sources_config as onboarded_config,
        c.config_hash as current_config_hash
    FROM {{ ref('int_platform_observability__source_candidate_map') }} r
    LEFT JOIN latest_config c ON r.client_slug = c.tenant_slug
),

final_logic AS (
    SELECT 
        client_slug,
        source_schema,
        source_table,
        created_at,
        integrated_flag,
        dbt_freshness_status,
        candidate_priority_score,
        
        -- The "Action" logic based on prioritized discovery requirements
        CASE 
            -- Schema Drift / Staleness Check
            WHEN integrated_flag = 'true' 
                 AND source_schema = 'airbyte' 
                 AND EXISTS (
                     SELECT 1 
                     FROM UNNEST(json_transform(onboarded_config->('$.' || source_table), '["JSON"]')) AS t(source)
                     WHERE json_extract_string(source, '$.schema') = 'metricmaven'
                 ) THEN 'RE-ONBOARD (SWAP TO METRICMAVEN)'
                 
            WHEN candidate_priority_score IN ('Strong Candidate - Active', 'Strong Candidate - New Shell') 
                 AND integrated_flag = 'false' THEN 'ONBOARD'
                 
            WHEN candidate_priority_score = 'Integrated - Stale/Failing' THEN 'INVESTIGATE SWAP'
            
            ELSE 'MONITOR'
        END as recommended_action
    FROM registry
    WHERE tenant_status IN ('pending', 'enabled')
)

SELECT * FROM final_logic
ORDER BY 
    CASE WHEN recommended_action LIKE 'RE-ONBOARD%' THEN 1
         WHEN recommended_action = 'ONBOARD' THEN 2
         WHEN recommended_action = 'INVESTIGATE SWAP' THEN 3
         ELSE 4 END,
    created_at DESC