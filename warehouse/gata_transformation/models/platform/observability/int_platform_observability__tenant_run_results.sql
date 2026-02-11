{{ config(materialized='table') }}

WITH tenant_models AS (
    SELECT
        invocation_id,
        dlt_load_id,
        node_id,
        node_name,
        run_result_status,
        rows_affected,
        execution_time_seconds,
        run_started_at,
        CASE
            WHEN node_name LIKE '%tyrell_corp%' THEN 'tyrell_corp'
            WHEN node_name LIKE '%wayne_enterprises%' THEN 'wayne_enterprises'
            WHEN node_name LIKE '%stark_industries%' THEN 'stark_industries'
            ELSE NULL
        END AS tenant_slug
    FROM {{ ref('int_platform_observability__run_results') }}
)

SELECT
    tenant_slug,
    node_name AS model_name,
    run_result_status AS status,
    rows_affected,
    execution_time_seconds,
    run_started_at,
    dlt_load_id
FROM tenant_models
WHERE tenant_slug IS NOT NULL
