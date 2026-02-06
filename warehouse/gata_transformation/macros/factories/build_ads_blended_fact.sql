{% macro build_ads_blended_fact(master_model_id) %}

SELECT
    h.hub_key,
    h.platform_name as source_platform, -- Allows mixed sources in one MM (FB/IG)
    h.raw_data_payload,
    h.source_schema_hash, -- Essential for future schema migration logic
    h.loaded_at
FROM {{ ref('hub_tenant_sources') }} h
WHERE h.master_model_ref = '{{ master_model_id }}'

{% endmacro %}
