{% macro sync_to_schema_history() %}
    -- Only run on execute to avoid compile-time issues
    {% if execute %}
        {% set target_table = 'platform_sat__source_schema_history' %}
        
        -- Generate the payload from the current model context
        {% set tenant_slug = this.schema.split('_')[0] if '_' in this.schema else 'unknown' %} 
        -- Note: Current tenant slug extraction is brittle, better to grab from columns if possible
        -- The prompt says "Capture schema metadata from {{ this }}".
        -- Since this is running in the context of the Staging Model, we can select from {{ this }}
        
        -- However, the macro runs *after* the model runs, so we can query the model itself.
        -- Or we can just use the values we know are in the model.
        
        {% set merge_query %}
            MERGE INTO {{ ref('platform_sat__source_schema_history') }} as target
            USING (
                SELECT DISTINCT
                    tenant_slug,
                    source_platform,
                    replace(_src_table, source_platform || '_', '') as source_table_name,
                    source_schema_hash,
                    source_schema::JSON as source_schema,
                    {{ dbt_utils.generate_surrogate_key([
                        'tenant_slug', 
                        'source_platform', 
                        "replace(_src_table, source_platform || '_', '')", 
                        'source_schema_hash'
                    ]) }} as source_schema_skey,
                    current_timestamp as now_ts
                FROM {{ this }}
            ) as source
            ON target.source_schema_skey = source.source_schema_skey
            
            WHEN MATCHED THEN
                UPDATE SET updated_at = source.now_ts
                
            WHEN NOT MATCHED THEN
                INSERT (
                    tenant_slug,
                    platform_name,
                    source_table_name,
                    source_schema_hash,
                    source_schema,
                    source_schema_skey,
                    first_seen_at,
                    updated_at
                ) VALUES (
                    source.tenant_slug,
                    source.source_platform,
                    source.source_table_name,
                    source.source_schema_hash,
                    source.source_schema,
                    source.source_schema_skey,
                    source.now_ts,
                    source.now_ts
                );
        {% endset %}
        
        {% do run_query(merge_query) %}
        {% do log("Synced schema history for " ~ this, info=True) %}
    {% endif %}
{% endmacro %}
