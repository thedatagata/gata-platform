{{
    config(
        materialized='incremental',
        unique_key=['invocation_id', 'test_node_id'],
        full_refresh=false
    )
}}

with source as (

    select * from {{ ref('src_platform_observability__test_definitions') }}

),

renamed as (

    select
        invocation_id,
        node_id as test_node_id,
        test_name,
        test_type,
        description as test_description,
        
        -- Parse JSON fields
        try_cast(tags as JSON) as tags_json,
        try_cast(test_metadata as JSON) as test_metadata_json,
        try_cast(target_model_ids as JSON) as target_model_ids,
        
        extracted_at

    from source

)

select * from renamed