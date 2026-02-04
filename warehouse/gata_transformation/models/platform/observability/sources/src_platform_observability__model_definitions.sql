{{
    config(
        materialized='ephemeral'
    )
}}

select * from {{ source('dbt_artifacts', 'model_artifacts__current') }}