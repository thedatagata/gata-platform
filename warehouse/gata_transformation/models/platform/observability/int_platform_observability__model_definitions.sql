{{ config(materialized='table') }}

SELECT
    model_node_id as node_id,
    model_name,
    materialization,
    configured_schema as dbt_schema,
    tags_json as tags,
    upstream_node_ids as depends_on_nodes,
    extracted_at as defined_at
FROM {{ ref('stg_platform_observability__model_definitions') }}