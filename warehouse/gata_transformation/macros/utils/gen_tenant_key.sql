{% macro generate_tenant_key(client_slug, platform_name=none, updated_at=none, config_hash=none) %}
    {%- set parts = [client_slug] -%}
    {%- if platform_name -%}
        {%- do parts.append(platform_name) -%}
    {%- endif -%}
    {%- if updated_at -%}
        {%- do parts.append(updated_at) -%}
    {%- endif -%}
    {%- if config_hash -%}
        {%- do parts.append(config_hash) -%}
    {%- endif -%}
    {{ dbt_utils.generate_surrogate_key(parts) }}
{% endmacro %}
