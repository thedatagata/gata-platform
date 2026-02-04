{%- macro get_client_logic(client_slug, logic_key, table_alias=none) -%}
{#
    Extracts a specific logic value from a tenant's config.
    Searches tenant-level client_logic first, then source-level logic blocks.
    
    Returns SQL expression:
    - If value starts with '$': extracts from raw_data_payload JSON
    - Otherwise: returns the literal value
    - If not found: returns CAST(NULL AS VARCHAR)
#}
    {%- set tenant_cfg = get_tenant_config(client_slug) -%}
    {%- set logic_value = tenant_cfg.get('client_logic', {}).get(logic_key) if tenant_cfg else none -%}
    
    {%- if not logic_value and tenant_cfg -%}
        {%- for source_name, source_config in tenant_cfg.get('sources', {}).items() -%}
            {%- if source_config is mapping and source_config.get('logic', {}).get(logic_key) -%}
                {%- set logic_value = source_config.logic.get(logic_key) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

    {%- if logic_value -%}
        CASE 
            WHEN {{ table_alias ~ '.' if table_alias }}tenant_slug = '{{ client_slug }}' 
            THEN 
                {%- if logic_value is string and logic_value.startswith('$') -%}
                    {{ table_alias ~ '.' if table_alias }}raw_data_payload->>'{{ logic_value[2:] }}'
                {%- else -%}
                    '{{ logic_value }}'
                {%- endif %}
            ELSE NULL 
        END
    {%- else -%}
        CAST(NULL AS VARCHAR)
    {%- endif -%}
{%- endmacro -%}
