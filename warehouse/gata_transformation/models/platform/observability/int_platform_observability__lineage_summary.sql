{{ config(materialized='table') }}

SELECT
    run.dlt_load_id,
    run.node_name AS dbt_model,
    run.run_result_status,
    reg.source_platform AS connector_type,
    reg.source_table_name,
    reg.registered_at AS schema_registered_at,
    run.execution_time_seconds,
    run.run_started_at
FROM {{ ref('int_platform_observability__run_results') }} run
INNER JOIN {{ ref('platform_ops__master_model_registry') }} reg
    ON run.node_name LIKE 'stg_%__' || reg.source_table_name
WHERE run.dlt_load_id IS NOT NULL
