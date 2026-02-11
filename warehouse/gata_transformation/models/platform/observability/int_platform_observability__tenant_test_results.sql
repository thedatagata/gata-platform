{{ config(materialized='table') }}

WITH tenant_tests AS (
    SELECT
        invocation_id,
        node_id,
        node_name,
        test_status,
        test_message,
        execution_time_seconds,
        run_started_at,
        CASE
            WHEN node_name LIKE '%tyrell_corp%' THEN 'tyrell_corp'
            WHEN node_name LIKE '%wayne_enterprises%' THEN 'wayne_enterprises'
            WHEN node_name LIKE '%stark_industries%' THEN 'stark_industries'
            ELSE NULL
        END AS tenant_slug
    FROM {{ ref('int_platform_observability__test_results') }}
)

SELECT
    tenant_slug,
    node_name AS test_name,
    test_status AS status,
    test_message AS message,
    execution_time_seconds,
    run_started_at
FROM tenant_tests
WHERE tenant_slug IS NOT NULL
