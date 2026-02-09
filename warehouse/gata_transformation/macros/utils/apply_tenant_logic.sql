{% macro apply_tenant_logic(tenant_slug, source_name, object_name, type='filter') %}
    {# 
       Refactored to handle multi-tenant logic injection.
       Iterates over `var('tenant_configs')` (list of tenants) to generate CASE statements.
    #}
    
    {%- set tenant_configs = var('tenant_configs', []) -%}
    {# Support both list (direct var) and dict wrapper if dbt passes it as default dict #}
    {%- if tenant_configs is mapping -%}
        {%- set all_tenants = tenant_configs.get('tenants', []) -%}
    {%- else -%}
        {%- set all_tenants = tenant_configs -%}
    {%- endif -%}

    {%- if tenant_slug is none -%}
        {# GLOBAL MODE: Iterate all tenants #}
        
        {%- if type == 'calculation' -%}
            {# 
               1. Scan for calculations to group by Alias. 
               We need to generate one CASE statement per Alias (field name).
            #}
            {%- set calcs_map = {} -%}
            
            {%- for tenant in all_tenants -%}
                {%- set t_slug = tenant.slug -%}
                {# Traverse: sources -> source_name -> tables -> object_name -> logic -> calculations #}
                {%- set src = tenant.get('sources', {}).get(source_name) -%}
                {%- if src -%}
                    {%- set tables = src.get('tables', []) -%}
                    {# Find the table config #}
                    {%- for tbl in tables -%}
                        {%- if tbl.name == object_name and tbl.get('logic', {}).get('calculations') -%}
                            {%- for calc in tbl.logic.calculations -%}
                                {%- set alias = calc.alias -%}
                                {%- set formula = calc.formula -%}
                                {%- if alias not in calcs_map -%}
                                    {%- do calcs_map.update({alias: []}) -%}
                                {%- endif -%}
                                {%- do calcs_map[alias].append({'slug': t_slug, 'formula': formula}) -%}
                            {%- endfor -%}
                        {%- endif -%}
                    {%- endfor -%}
                {%- endif -%}
            {%- endfor -%}
            
            {# 2. Generate CASE statements #}
            {%- for alias, rules in calcs_map.items() -%}
                , CASE 
                    {%- for rule in rules -%}
                        WHEN tenant_slug = '{{ rule.slug }}' THEN {{ rule.formula }}
                    {%- endfor -%}
                    ELSE NULL
                  END as {{ alias }}
            {%- endfor -%}

        {%- elif type == 'filter' -%}
            {# 
               Generate ONE filter block:
               AND CASE 
                   WHEN tenant='A' THEN (filterA)
                   WHEN tenant='B' THEN (filterB)
                   ELSE TRUE 
               END
            #}
             {%- set has_filters = false -%}
             
             {# Buffer the WHEN clauses #}
             {%- set when_clauses = [] -%}
             
             {%- for tenant in all_tenants -%}
                {%- set t_slug = tenant.slug -%}
                {%- set src = tenant.get('sources', {}).get(source_name) -%}
                {%- if src -%}
                    {%- set tables = src.get('tables', []) -%}
                    {%- for tbl in tables -%}
                        {%- if tbl.name == object_name and tbl.get('logic', {}).get('filters') -%}
                             {%- set filter_condition = tbl.logic.filters | join(' AND ') -%}
                             {%- do when_clauses.append("WHEN tenant_slug = '" ~ t_slug ~ "' THEN (" ~ filter_condition ~ ")") -%}
                        {%- endif -%}
                    {%- endfor -%}
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
        {# SINGLE TENANT MODE (Legacy/Specific) #}
        {# This path is used if tenant_slug IS passed (e.g. for specific hydration if needed) #}
        {# Assuming we only use global mode for the main union, but keeping this for safety #}
    {%- endif -%}
{% endmacro %}
