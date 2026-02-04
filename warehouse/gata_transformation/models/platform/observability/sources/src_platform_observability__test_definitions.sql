{{
    config(
        materialized='ephemeral'
    )
}}

select * from {{ source('dbt_artifacts', 'test_artifacts__current') }}