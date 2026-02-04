{{
    config(
        materialized='ephemeral'
    )
}}

select * from {{ source('dbt_artifacts', 'run_results__current') }}