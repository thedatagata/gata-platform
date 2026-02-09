{% macro generate_master_union(master_type) %}
    {# 
       Dynamically find all hubs for this concept using the registry check.
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
            {# Construct physical table name from registry info #}
            {%- set table_name = 'platform_mm__' ~ platform ~ '_api_v1_' ~ row[1] -%}
            
            SELECT 
                *,
                '{{ platform }}' as source_platform
                
                {# Inject custom metrics (Calculations) #}
                {{ apply_tenant_logic(none, platform, master_type, 'calculation') }}
                
            FROM {{ ref(table_name) }}
            WHERE 1=1
            
            {# Inject filters #}
            {{ apply_tenant_logic(none, platform, master_type, 'filter') }}
            
            {%- if not loop.last %} UNION ALL {% endif -%}
        {%- endfor -%}
    {%- else -%}
        SELECT 'No hubs found for {{ master_type }}' as error
    {%- endif -%}
{% endmacro %}
