{% macro apply_tenant_logic(tenant_slug, source_name, object_name, type='filter') %}
    {# 1. Get full tenant config #}
    {%- set tenant_cfg = get_tenant_config(tenant_slug) -%}
    
    {# 2. Find the specific table's logic #}
    {%- set table_logic = {} -%}
    
    {%- if tenant_cfg -%}
         {%- set source_cfg = tenant_cfg.get('sources', {}).get(source_name, {}) -%}
         {%- if source_cfg -%}
             {%- set tables = source_cfg.get('tables', []) -%}
             {%- for table in tables -%}
                 {%- if table.name == object_name -%}
                     {%- set table_logic = table.logic -%}
                 {%- endif -%}
             {%- endfor -%}
         {%- endif -%}
    {%- endif -%}

    {# 3. Inject logic based on type #}
    {%- if type == 'calculation' -%}
        {%- if table_logic and table_logic.calculations -%}
             {%- for calc in table_logic.calculations -%}
                 , {{ calc.formula }} as {{ calc.alias }}
             {%- endfor -%}
        {%- endif -%}
    {%- elif type == 'filter' -%}
        {%- if table_logic and table_logic.filters -%}
             AND {{ table_logic.filters | join(' AND ') }}
        {%- endif -%}
    {%- endif -%}

{% endmacro %}
