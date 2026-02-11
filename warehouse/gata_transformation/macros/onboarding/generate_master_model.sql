{#
  Master Model Generator
  
  Creates incremental sink tables with a fixed 7-column contract.
  Data flows in via sync_to_master_hub() MERGE post-hooks on staging models,
  NOT through dbt's incremental append (the SELECT returns 0 rows by design).

  CRITICAL: full_refresh=false prevents --full-refresh from dropping historical data.
  Master models are append-only sinks — they must never be recreated.
  To truly reset a master model, drop the table manually in the warehouse.
#}
{% macro generate_master_model() %}
{{ config(
    materialized='incremental',
    full_refresh=false,
    on_schema_change='append_new_columns',
    tags=['master_model']
) }}

SELECT
    CAST(NULL AS VARCHAR) as tenant_slug,
    CAST(NULL AS VARCHAR) as tenant_skey,
    CAST(NULL AS VARCHAR) as source_platform,
    CAST(NULL AS VARCHAR) as source_schema_hash,
    CAST(NULL AS JSON) as source_schema,
    CAST(NULL AS JSON) as raw_data_payload,
    CAST(NULL AS TIMESTAMP) as loaded_at
WHERE 1=0
{% endmacro %}