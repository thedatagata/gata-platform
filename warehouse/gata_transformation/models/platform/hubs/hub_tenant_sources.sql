{{ config(materialized='table') }}

{%- set all_clients = get_all_clients() -%}

{%- set all_sources = [] -%}
{%- for client in all_clients -%}
    {%- set sources = client.get('sources', {}) -%}
    {%- for platform, platform_sources in sources.items() -%}
        {%- do all_sources.append({'slug': client.slug, 'platform': platform}) -%}
    {%- endfor -%}
{%- endfor -%}

WITH source_seed AS (
    {% for s in all_sources -%}
    SELECT 
        '{{ s.slug }}' as client_slug,
        '{{ s.platform }}' as platform_name,
        {{ generate_tenant_key("'" ~ s.slug ~ "'") }} as tenant_skey
    {{ "UNION ALL " if not loop.last }}
    {%- endfor %}
)

SELECT
    {{ generate_tenant_key('client_slug', 'platform_name') }} as hub_source_key,
    client_slug,
    platform_name,
    tenant_skey,
    now() as created_at
FROM source_seed