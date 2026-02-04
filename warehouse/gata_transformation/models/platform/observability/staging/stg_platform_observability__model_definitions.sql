{{
    config(
        materialized='incremental',
        unique_key=['invocation_id', 'model_node_id'],
        full_refresh=false
    )
}}

with source as (

    select * from {{ ref('src_platform_observability__model_definitions') }}

),

renamed as (

    select
        invocation_id,
        node_id as model_node_id,
        model_name,
        description as model_description,
        materialization,
        schema_name as configured_schema,
        tags,
        depends_on_nodes,
        
        -- Parse JSON fields
        try_cast(tags as JSON) as tags_json,
        try_cast(meta as JSON) as meta_json,
        try_cast(config as JSON) as config_json,
        try_cast(depends_on_nodes as JSON) as upstream_node_ids,
        
        extracted_at

    from source

)

select * from renamed