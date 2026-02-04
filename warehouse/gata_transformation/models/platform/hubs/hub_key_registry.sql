{{ config(materialized='table') }}

WITH history AS (
    SELECT 
        tenant_slug,
        updated_at,
        config_hash,
        tenant_skey
    FROM {{ ref('platform_sat__tenant_config_history') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tenant_slug ORDER BY updated_at DESC) = 1
),

manifest_seeds AS (
    {%- set all_clients = get_all_clients() -%}
    {%- for client in all_clients %}
    SELECT 
        '{{ client.slug }}' as client_slug, 
        CAST(NULL AS VARCHAR) as platform_name
    
    {%- set platforms = client.get('sources', {}).keys() -%}
    {%- for platform in platforms %}
    UNION ALL
    SELECT 
        '{{ client.slug }}' as client_slug, 
        '{{ platform }}' as platform_name
    {%- endfor %}
    
    {%- if not loop.last %}
    UNION ALL
    {%- endif %}
    {%- endfor %}
)

SELECT 
    s.client_slug,
    s.platform_name,
    {{ generate_tenant_key('s.client_slug', 's.platform_name', 'h.updated_at', 'h.config_hash') }} as generated_hash,
    h.updated_at as logic_version_at,
    h.config_hash as logic_config_hash,
    now() as logged_at
FROM manifest_seeds s
LEFT JOIN history h ON s.client_slug = h.tenant_slug