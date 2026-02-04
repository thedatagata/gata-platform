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
            INSERT (
                tenant_slug,
                tenant_skey,
                source_platform,
                source_schema_hash,
                source_schema,
                raw_data_payload
            )
            VALUES (
                source.tenant_slug,
                source.tenant_skey,
                source.source_platform,
                source.source_schema_hash,
                source.source_schema,
                source.raw_data_payload
            )
    {%- endset -%}
    
    {%- if execute -%}
        {{ log("🔄 Syncing " ~ source_relation ~ " -> " ~ target_relation, info=True) }}
        {%- do run_query(query) -%}
    {%- endif -%}

{% endmacro %}
