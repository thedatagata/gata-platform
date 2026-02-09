{% macro generate_master_union(master_type) %}
    {# 
       Dynamically find all hubs for this concept using the registry check.
       This replaces the hardcoded dictionary with a dynamic query.
    #}
    {%- set models_query -%}
        SELECT source_platform, source_table_name, master_model_id
        FROM {{ ref('platform_ops__master_model_registry') }}
        WHERE master_model_id LIKE '%{{ master_type }}%'
    {%- endset -%}

    {%- set results = run_query(models_query) if execute else [] -%}

    {%- if results | length > 0 -%}
        {%- for row in results -%}
            {%- set platform = row[0] -%}
            {# Reconstruct table name or use registry info. 
               The registry has (platform, source_table_name, master_model_id).
               The physical table name usually follows `platform_mm__<platform>_api_v1_<source_table_name>`.
               Let's construct it, or better, the registry should probably store the full physical table name.
               Assuming standard naming convention:
            #}
            {%- set table_name = 'platform_mm__' ~ platform ~ '_api_v1_' ~ row[1] -%}
            
            SELECT 
                *,
                '{{ platform }}' as source_platform
                
                {# Inject custom metrics from tenants.yaml while the platform name is known #}
                {{ apply_tenant_logic(none, platform, master_type, 'calculation') }}
                
            FROM {{ ref(table_name) }}
            WHERE 1=1
            
            {# Inject filters while platform is known #}
            {{ apply_tenant_logic(none, platform, master_type, 'filter') }}
            
            {%- if not loop.last %} UNION ALL {% endif -%}
        {%- endfor -%}
    {%- else -%}
        {# Fallback for safety or compilation phase #}
        SELECT 'No hubs found for {{ master_type }}' as error
    {%- endif -%}
{% endmacro %}
