{% macro generate_staging_pusher(tenant_slug, source_name, schema_hash, master_model_id, source_table) %}
{{ config(
    materialized='view',
    post_hook=["{{ sync_to_master_hub('" ~ master_model_id ~ "') }}"]
) }}

WITH base AS (
    SELECT * FROM {{ source(tenant_slug ~ '_' ~ source_name, source_table) }}
)
SELECT
    '{{ tenant_slug }}'::VARCHAR as tenant_slug,
    {{ generate_tenant_key("'" ~ tenant_slug ~ "'") }} as tenant_skey,
    '{{ source_name }}'::VARCHAR as source_platform,
    '{{ schema_hash }}'::VARCHAR as source_schema_hash,
    CAST(NULL AS JSON) as source_schema,
    -- Standardizing raw data payload by re-packing normalized columns
    row_to_json(base) as raw_data_payload,
    current_timestamp as loaded_at
FROM base
{% endmacro %}