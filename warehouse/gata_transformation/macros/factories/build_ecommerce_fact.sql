{% macro build_ecommerce_fact(master_model_id) %}

SELECT
    h.hub_key,
    h.platform_name as source_platform,
    h.raw_data_payload,
    h.source_schema_hash,
    h.loaded_at
FROM {{ ref('hub_tenant_sources') }} h
WHERE h.master_model_ref = '{{ master_model_id }}'

{% endmacro %}
