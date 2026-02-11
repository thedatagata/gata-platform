{{ config(materialized='view') }}

WITH latest_runs AS (
    SELECT
        dlt_load_id,
        node_name,
        run_result_status,
        ROW_NUMBER() OVER (
            PARTITION BY node_name
            ORDER BY run_started_at DESC
        ) AS run_rank
    FROM {{ ref('int_platform_observability__run_results') }}
    WHERE run_result_status IS NOT NULL
),

semantic_models AS (
    SELECT DISTINCT
        tenant_slug,
        subject,
        table_name
    FROM {{ ref('platform_ops__boring_semantic_layer') }}
)

SELECT
    sm.tenant_slug,
    sm.subject,
    lr.dlt_load_id,
    CASE
        WHEN lr.run_result_status = 'success' THEN TRUE
        ELSE FALSE
    END AS is_semantic_layer_ready,
    lr.run_result_status AS last_dbt_status
FROM semantic_models sm
LEFT JOIN latest_runs lr
    ON lr.node_name = sm.table_name
    AND lr.run_rank = 1
WHERE lr.run_rank = 1 OR lr.run_rank IS NULL
