{{ config(materialized='table', tags=["onboarding", "platform"]) }}

{%- set platforms = ['facebook_ads', 'google_ads', 'google_analytics', 'shopify', 'stripe'] -%}

WITH latest_history AS (
    SELECT 
        tenant_skey,
        tenant_slug,
        sources_config,
        updated_at
    FROM {{ ref('platform_sat__tenant_config_history') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tenant_slug ORDER BY updated_at DESC) = 1
),

flattened_platforms AS (
    {%- for platform in platforms %}
    SELECT
        h.tenant_skey,
        h.tenant_slug,
        '{{ platform }}' as platform_name,
        (h.sources_config -> '$.{{ platform }}') as platform_json
    FROM latest_history h
    {%- if not loop.last %} UNION ALL {% endif %}
    {%- endfor %}
)

SELECT
    tenant_skey,
    tenant_slug,
    platform_name,
    (t ->> 'table_name') as table_name,
    (t ->> 'dataset') as dataset,
    (t ->> 'project') as project,
    (t ->> 'filter_col') as filter_col,
    (t ->> 'filter_val') as filter_val
FROM flattened_platforms,
UNNEST(CAST(json_extract(platform_json, '$.tables') AS JSON[])) as t
WHERE platform_json IS NOT NULL