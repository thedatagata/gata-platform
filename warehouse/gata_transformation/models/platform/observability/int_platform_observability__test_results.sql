{{ config(materialized='incremental', unique_key=['invocation_id', 'node_id']) }}

SELECT
    invocation_id,
    node_id,
    node_name,
    run_result_status as test_status,
    run_result_message as test_message,
    execution_time_seconds,
    run_started_at
FROM {{ ref('stg_platform_observability__run_results') }}
WHERE resource_type = 'test'
{% if is_incremental() %}
  AND run_started_at > (SELECT MAX(run_started_at) FROM {{ this }})
{% endif %}