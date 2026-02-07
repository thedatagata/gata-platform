{% macro sync_to_master_hub(master_model_id) %}
    {%- set target_relation = ref('platform_mm__' ~ master_model_id) -%}
    {%- set source_relation = this -%}

    {%- set query -%}
        MERGE INTO {{ target_relation }} AS target
        USING {{ source_relation }} AS source
        ON target.tenant_slug = source.tenant_slug
        AND target.source_platform = source.source_platform
        AND md5(target.raw_data_payload::VARCHAR) = md5(source.raw_data_payload::VARCHAR)
        
        WHEN NOT MATCHED THEN
            INSERT (tenant_slug, hub_key, source_platform, source_schema_hash, raw_data_payload, loaded_at)
            VALUES (source.tenant_slug, source.hub_key, source.source_platform, source.source_schema_hash, source.raw_data_payload, current_timestamp)
    {%- endset -%}
    
    {%- if execute -%}
        {{ log("🚀 Hardcoded Push: " ~ source_relation ~ " -> " ~ target_relation, info=True) }}
        {%- do run_query(query) -%}
    {%- endif -%}
{% endmacro %}
