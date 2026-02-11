{%- macro get_tenant_config(tenant_key, source_name=none) -%}
{#
    Returns tenant configuration from tenants.yaml.
    PRIORITY:
    1. dbt variable 'tenant_configs' (injected at runtime via main.py)
    2. manifest.tenants (parsed via custom adapter/loader if available)
#}
    {%- set tenant_configs_var = var('tenant_configs', none) -%}
    {%- if tenant_configs_var -%}
        {%- set tenants_list = tenant_configs_var.get('tenants', []) -%}
        {%- set tenant_cfg = tenants_list | selectattr("slug", "equalto", tenant_key) | first -%}
        {%- if tenant_cfg -%}
             {%- if source_name -%}
                 {{ return(tenant_cfg.get('sources', {}).get(source_name, {})) }}
             {%- else -%}
                 {{ return(tenant_cfg) }}
             {%- endif -%}
        {%- endif -%}
    {%- endif -%}

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
            'linkedin_ads': {'enabled': true, 'logic': {}},
            'bing_ads': {'enabled': true, 'logic': {}},
            'amazon_ads': {'enabled': true, 'logic': {}},
            'bigcommerce': {'enabled': true, 'logic': {}},
            'mixpanel': {'enabled': true, 'logic': {}}
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
