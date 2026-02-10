{% macro apply_tenant_logic(tenant_slug, source_name, object_name, type='filter') %}
    {# 
       Refactored Logic Injection:
       1. Iterates `var('tenant_configs')` for multi-tenant support.
       2. Uses specific path structure for Python config export:
          tenant.sources.<source_name>.logic.<object_name>
    #}
    
    {%- set tenant_configs = var('tenant_configs', []) -%}
    {%- if tenant_configs is mapping -%}
        {%- set all_tenants = tenant_configs.get('tenants', []) -%}
    {%- else -%}
        {%- set all_tenants = tenant_configs -%}
    {%- endif -%}

    {%- if tenant_slug is none -%}
        {# GLOBAL MODE #}
        
        {%- if type == 'calculation' -%}
            {# Aggregate calculations by alias #}
            {%- set calcs_map = {} -%}
            
            {%- for tenant in all_tenants -%}
                {%- set t_slug = tenant.slug -%}
                {# Traverse: sources -> source_name -> logic -> object_name -> calculations #}
                {%- set logic_node = tenant.get('sources', {}).get(source_name, {}).get('logic', {}).get(object_name, {}) -%}
                
                {%- if logic_node and logic_node.get('calculations') -%}
                    {%- for calc in logic_node.calculations -%}
                        {%- set alias = calc.alias -%}
                        {%- set formula = calc.formula -%}
                        {%- if alias not in calcs_map -%}
                            {%- do calcs_map.update({alias: []}) -%}
                        {%- endif -%}
                        {%- do calcs_map[alias].append({'slug': t_slug, 'formula': formula}) -%}
                    {%- endfor -%}
                {%- endif -%}
            {%- endfor -%}
            
            {%- for alias, rules in calcs_map.items() -%}
                , CASE 
                    {%- for rule in rules -%}
                        WHEN tenant_slug = '{{ rule.slug }}' THEN {{ rule.formula }}
                    {%- endfor -%}
                    ELSE NULL
                  END as {{ alias }}
            {%- endfor -%}

        {%- elif type == 'filter' -%}
             {%- set when_clauses = [] -%}
             {%- for tenant in all_tenants -%}
                {%- set t_slug = tenant.slug -%}
                {# Traverse: sources -> source_name -> logic -> object_name -> filters #}
                {%- set logic_node = tenant.get('sources', {}).get(source_name, {}).get('logic', {}).get(object_name, {}) -%}
                
                {%- if logic_node and logic_node.get('filters') -%}
                     {%- set filter_condition = logic_node.filters | join(' AND ') -%}
                     {%- do when_clauses.append("WHEN tenant_slug = '" ~ t_slug ~ "' THEN (" ~ filter_condition ~ ")") -%}
                {%- endif -%}
             {%- endfor -%}
             
             {%- if when_clauses | length > 0 -%}
                 AND CASE
                     {%- for clause in when_clauses -%}
                         {{ clause }}
                     {%- endfor -%}
                     ELSE TRUE
                 END
             {%- endif -%}
        {%- endif -%}

    {%- else -%}
        {# SINGLE TENANT MODE (if needed) #}
    {%- endif -%}
{% endmacro %}
