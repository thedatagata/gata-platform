{% macro build_ads_blended_fact(master_model_id) %}

SELECT
    hub_key,
    source_platform, -- Allows mixed sources in one MM (FB/IG)
    raw_data_payload,
    source_schema_hash, -- Essential for future schema migration logic
    loaded_at
FROM {{ ref('platform_mm__' ~ master_model_id) }}

{% endmacro %}
