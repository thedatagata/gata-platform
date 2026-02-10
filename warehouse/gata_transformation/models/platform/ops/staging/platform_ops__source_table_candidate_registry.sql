{{ config(materialized='table') }}

WITH latest_config AS (
    SELECT
        tenant_slug,
        source_name,
        table_name,
        table_logic,
        logic_hash
    FROM {{ ref('platform_sat__tenant_config_history') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tenant_slug, source_name, table_name ORDER BY updated_at DESC) = 1
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
        c.logic_hash as current_logic_hash
    FROM {{ ref('int_platform_observability__source_candidate_map') }} r
    LEFT JOIN latest_config c
        ON r.client_slug = c.tenant_slug
        AND r.source_schema = c.source_name
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

        CASE
            WHEN candidate_priority_score IN ('Strong Candidate - Active', 'Strong Candidate - New Shell')
                 AND integrated_flag = 'false' THEN 'ONBOARD'

            WHEN candidate_priority_score = 'Integrated - Stale/Failing' THEN 'INVESTIGATE SWAP'

            ELSE 'MONITOR'
        END as recommended_action
    FROM registry
    WHERE tenant_status IN ('pending', 'enabled', 'onboarding')
)

SELECT * FROM final_logic
ORDER BY
    CASE WHEN recommended_action = 'ONBOARD' THEN 1
         WHEN recommended_action = 'INVESTIGATE SWAP' THEN 2
         ELSE 3 END,
    created_at DESC