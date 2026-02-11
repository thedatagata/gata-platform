{{
    config(
        materialized='incremental',
        unique_key=['invocation_id', 'node_id'],
        full_refresh=false
    )
}}

with source as (

    select * from {{ ref('src_platform_observability__run_results') }}

),

renamed as (

    select
        invocation_id,
        node_id,
        node_name,
        resource_type,
        status as run_result_status,
        message as run_result_message,
        rows_affected,
        execution_time_seconds,
        run_started_at,
        run_completed_at,
        extracted_at

    from source
    qualify row_number() over (partition by invocation_id, node_id order by run_started_at desc) = 1

)

select 
    invocation_id,
    cast('{{ var("dlt_load_id", "manual_run") }}' as varchar) as dlt_load_id,
    node_id,
    node_name,
    resource_type,
    run_result_status,
    run_result_message,
    rows_affected,
    execution_time_seconds,
    run_started_at,
    run_completed_at,
    extracted_at
from renamed