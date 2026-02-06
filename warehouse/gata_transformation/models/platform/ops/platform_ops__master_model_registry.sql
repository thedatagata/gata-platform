{{ config(materialized='table') }}

-- This model now strictly represents the definitive Library Registry
-- created by the initialize_connector_library script.
SELECT
    source_schema_hash,
    source_name as source_platform,
    source_table_name,
    master_model_id,
    version as blueprint_version,
    registered_at
FROM {{ source('connectors', 'connector_blueprints') }}
