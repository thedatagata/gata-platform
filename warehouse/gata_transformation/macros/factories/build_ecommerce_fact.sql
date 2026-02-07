{% macro build_ecommerce_fact(master_model_id) %}

SELECT
    hub_key,
    source_platform,
    raw_data_payload,
    source_schema_hash,
    loaded_at
FROM {{ ref('platform_mm__' ~ master_model_id) }}

{% endmacro %}
