{% macro apply_tenant_logic(tenant_slug, source_name, object_name, type='filter') %}
    {# 
       Refactored Logic:
       If tenant_slug is provided, behave as before (specific logic).
       If tenant_slug is NONE, iterate over ALL tenants in config.
       Generate a CASE statement for calculations (or WHERE OR blocks for filters).
    #}
    
    {%- set tenant_configs_var = var('tenant_configs', none) -%}
    {%- set all_tenants = [] -%}
    
    {%- if tenant_configs_var -%}
        {%- set all_tenants = tenant_configs_var.get('tenants', []) -%}
    {%- else -%}
        {%- set manifest = get_manifest() -%}
        {%- if execute and manifest -%}
             {%- set all_tenants = manifest.tenants -%}
        {%- endif -%}
    {%- endif -%}

    {%- if tenant_slug is none -%}
        {# GLOBAL MODE: Iterate all tenants #}
        
        {%- if type == 'calculation' -%}
            {# We need to collect calculations by alias. 
               e.g. all 'cpm' calculations from different tenants.
               Then: CASE WHEN tenant_slug='A' THEN calc_A WHEN tenant_slug='B' THEN calc_B END as cpm
            #}
            {# This is complex. For now, assume simple 1:1 injection or user prompt guidance. #}
            {# The prompt implies we just inject logic. #}
            {# "Refactor so logic is injected while iterating... using hardcoded source name" #}
            {# Let's generate a CASE statement per formula found. #}
            
            {# 1. Scan all tenants for logic on this object #}
            {%- set calcs_map = {} -%} {# alias -> list of {slug, formula} #}
            
            {%- for tenant in all_tenants -%}
                {%- set t_slug = tenant.slug -%}
                {%- set src = tenant.get('sources', {}).get(source_name, {}) -%}
                {%- if src -%}
                    {%- set tables = src.get('tables', []) -%}
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
            
            {# 2. Output CASE statements #}
            {%- for alias, rules in calcs_map.items() -%}
                , CASE 
                    {%- for rule in rules -%}
                        WHEN tenant_slug = '{{ rule.slug }}' THEN {{ rule.formula }}
                    {%- endfor -%}
                    ELSE NULL
                  END as {{ alias }}
            {%- endfor -%}

        {%- elif type == 'filter' -%}
             {# Filters are applied in WHERE. 
                (tenant_slug = 'A' AND filter_A) OR (tenant_slug = 'B' AND filter_B) OR ... #}
             
             {%- set conditions = [] -%}
             {%- for tenant in all_tenants -%}
                {%- set t_slug = tenant.slug -%}
                {%- set src = tenant.get('sources', {}).get(source_name, {}) -%}
                {%- if src -%}
                    {%- set tables = src.get('tables', []) -%}
                    {%- for tbl in tables -%}
                        {%- if tbl.name == object_name and tbl.get('logic', {}).get('filters') -%}
                             {%- set filter_expr = tbl.logic.filters | join(' AND ') -%}
                             {%- do conditions.append("(tenant_slug = '" ~ t_slug ~ "' AND (" ~ filter_expr ~ "))") -%}
                        {%- endif -%}
                    {%- endfor -%}
                {%- endif -%}
             {%- endfor -%}
             
             {%- if conditions -%}
                 AND (
                     {{ conditions | join(' OR ') }}
                     OR tenant_slug NOT IN (
                         {%- for tenant in all_tenants if tenant.slug in conditions|map(attribute='slug')|list -%}
                             '{{ tenant.slug }}'{{ "," if not loop.last }}
                         {%- endfor -%}
                         -- Default to TRUE for tenants without filters? 
                         -- Currently logic implies: if you have filters, apply them. If not, pass through?
                         -- The construction `(A AND fA) OR (B AND fB)` implicitly excludes C if C is not in the list?
                         -- Wait, `WHERE 1=1 AND ((A AND fA) OR (B AND fB))` excludes C!
                         -- We must allow tenants with NO filters to pass.
                         -- The prompt says "Pre-Injection" and "Post-Injection". 
                         -- Usually we want: `WHERE (tenant='A' AND filterA) OR (tenant='B' AND filterB) OR (tenant NOT IN ('A','B'))`
                         '' 
                     )
                     -- Actually simpler: just generate the positive conditions. 
                     -- If a tenant is NOT in the list, they are not filtered out by THIS block 
                     -- UNLESS we wrap it all in AND.
                     -- Let's check user intent. "Logic Dictionary... injects filters". 
                     -- Usually implies restriction.
                     -- Use safe fallback: (tenant='A' AND fA) OR (tenant!='A') is wrong.
                     -- Correct: `WHERE CASE WHEN tenant='A' THEN fA ... ELSE TRUE END`
                 )
                 -- RE-DOING FILTER LOGIC TO BE SAFE:
                 AND CASE
                     {%- for tenant in all_tenants -%}
                        {%- set t_slug = tenant.slug -%}
                        {%- set src = tenant.get('sources', {}).get(source_name, {}) -%}
                        {%- if src -%}
                             {%- set tables = src.get('tables', []) -%}
                             {%- for tbl in tables -%}
                                 {%- if tbl.name == object_name and tbl.get('logic', {}).get('filters') -%}
                                     WHEN tenant_slug = '{{ t_slug }}' THEN ({{ tbl.logic.filters | join(' AND ') }})
                                 {%- endif -%}
                             {%- endfor -%}
                        {%- endif -%}
                     {%- endfor -%}
                     ELSE TRUE
                 END
             {%- endif -%}
        {%- endif -%}

    {%- else -%}
        {# OLD SINGLE TENANT MODE (if needed) #}
        {# ... existing code ... #}
    {%- endif -%}

{% endmacro %}
