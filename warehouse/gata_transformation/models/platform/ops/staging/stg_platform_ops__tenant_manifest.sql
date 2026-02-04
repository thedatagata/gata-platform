{{ config(materialized='view') }}

{% set manifest = get_manifest() %}

WITH source AS (
    {% for tenant in manifest.tenants %}
    SELECT
        '{{ tenant.slug }}' as tenant_slug,
        '{{ tenant.status }}' as status,
        '{{ tenant.sources | tojson | replace("\'", "\'\'") }}'::JSON as sources_config,
        now() as loaded_at
    {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
)

SELECT
    tenant_slug,
    status,
    sources_config,
    md5(sources_config::VARCHAR) as config_hash,
    loaded_at
FROM source