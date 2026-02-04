{%- macro get_tenant_config(tenant_key, source_name=none) -%}
{#
    Returns tenant configuration from tenants.yaml.
    
    tenants.yaml structure:
    tenants:
      - slug: tyrell_corp
        name: Tyrell Corporation
        status: active
        sources:
          facebook_ads:
            enabled: true
            tables: [...]
            logic: { conversion_events: [...], ... }
          google_analytics:
            enabled: true
            tables: [...]
            logic: { conversion_events: ['purchase'], ... }
    
    Usage:
      get_tenant_config('tyrell_corp')              -> full tenant dict
      get_tenant_config('tyrell_corp', 'google_ads') -> google_ads source config dict
#}
{%- set manifest = get_manifest() -%}
{# Parse-time hack: Return a mock config with EVERYTHING enabled so dbt sees all potential refs #}
{%- if not execute or manifest is none -%}
    {%- set mock_sources = {
        'facebook_ads': {'enabled': true, 'logic': {}},
        'google_ads': {'enabled': true, 'logic': {}},
        'instagram_ads': {'enabled': true, 'logic': {}},
        'shopify': {'enabled': true, 'logic': {}},
        'stripe': {'enabled': true, 'logic': {}},
        'google_analytics': {'enabled': true, 'logic': {}},
        'woocommerce': {'enabled': true, 'logic': {}},
        'linkedin_ads': {'enabled': true, 'logic': {}}
    } -%}
    {%- if source_name -%}
        {{ return(mock_sources.get(source_name, {})) }}
    {%- else -%}
        {{ return({'sources': mock_sources}) }}
    {%- endif -%}
{%- endif -%}

{%- set tenant_cfg = manifest.tenants | selectattr("slug", "equalto", tenant_key) | first -%}
{%- if tenant_cfg and source_name -%}
    {{ return(tenant_cfg.get('sources', {}).get(source_name, {})) }}
{%- else -%}
    {{ return(tenant_cfg) }}
{%- endif -%}
{%- endmacro -%}
