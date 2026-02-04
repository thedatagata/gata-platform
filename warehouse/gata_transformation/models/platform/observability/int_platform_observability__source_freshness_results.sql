{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_freshness_result_skey',
    on_schema_change='append_new_columns'
  )
}}

with run_history as (

    select * from {{ ref('stg_platform_observability__run_results') }}
    where
        resource_type = 'FRESHNESS'

        {% if is_incremental() -%}
        and extracted_at > (select max(freshness_execution_started_at) from {{ this }})
        {%- endif %}

),

final as (

    select
        -- keys
        {{ dbt_utils.generate_surrogate_key(['h.invocation_id', 'h.node_id']) }} as dbt_freshness_result_skey,
        {{ dbt_utils.generate_surrogate_key(['h.node_id']) }} as dbt_source_key,
        -- BQ: Format date as integer key YYYYMMDD
        cast(strftime(h.run_started_at, '%Y%m%d') as bigint) as freshness_execution_started_date_key,

        -- identity
        h.node_id as source_node_id,
        split_part(h.node_id, '.', 3) as source_name,
        split_part(h.node_id, '.', 4) as source_table,
        h.node_name as source_node_name,
        h.invocation_id as dbt_invocation_id,

        -- properties
        h.run_result_status as freshness_status,
        h.run_result_message,
        
        -- facts: performance
        h.execution_time_seconds as freshness_check_duration_seconds,
        1 as count_freshness_checks,

        -- timestamps
        h.run_started_at as freshness_execution_started_at,

        -- metadata
        h.extracted_at as generated_at
    from
        run_history h

)

select * from final