{{ config(materialized='table') }}

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
        tags as tags_json,
        depends_on_nodes as upstream_node_ids,
        meta->>'$.owner' as model_owner,
        config->>'$.materialization' as config_materialization,
        extracted_at

    from source

)

select * from renamed