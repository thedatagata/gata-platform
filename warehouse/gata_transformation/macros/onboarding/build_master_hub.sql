{% macro build_master_hub(platform_name) %}

{%- set slugs = [] -%}
{%- set q -%}
    SELECT DISTINCT client_slug FROM {{ ref('hub_tenant_sources') }}
    WHERE platform_name = '{{ platform_name }}'
{%- endset -%}

{%- if execute -%}
    {%- set slugs = run_query(q).columns[0].values() -%}
{%- endif -%}

{%- set valid_stg_refs = [] -%}
{%- for s in slugs %}
    {%- set stg_ref = 'stg_' ~ s ~ '__' ~ platform_name -%}
    {%- set rel = load_relation(ref(stg_ref)) -%}
    {%- if rel is not none -%}
        {%- do valid_stg_refs.append(stg_ref) -%}
    {%- endif -%}
{%- endfor -%}

WITH union_base AS (
    {%- if valid_stg_refs | length > 0 -%}
        {%- for stg_ref in valid_stg_refs %}
            SELECT tenant_skey, raw_data_payload, source_schema
            FROM {{ ref(stg_ref) }}
            {%- if not loop.last %} UNION ALL {% endif -%}
        {%- endfor -%}
    {%- else -%}
        SELECT
            CAST(NULL AS VARCHAR) as tenant_skey,
            CAST(NULL AS JSON) as raw_data_payload,
            CAST(NULL AS JSON) as source_schema
        WHERE 1=0
    {%- endif -%}
)

SELECT * FROM union_base
{% endmacro %}

{% macro build_granular_master_hub(arg1, arg2=None) %}

{%- set staging_models = [] -%}

{%- if arg2 is not none -%}
    {# --- Legacy Mode --- #}
    {%- set platform_name = arg1 -%}
    {%- set table_suffix = arg2 -%}
    {%- set q -%}
        SELECT DISTINCT
            client_slug,
            'stg_' || client_slug || '__' || platform_name || '_' || '{{ table_suffix }}' as model_name
        FROM {{ ref('hub_tenant_sources') }}
        WHERE platform_name = '{{ platform_name }}'
    {%- endset -%}

    {%- if execute -%}
        {%- set results = run_query(q) -%}
        {%- for row in results -%}
            {%- set rel = adapter.get_relation(this.database, this.schema, row['model_name']) -%}
            {%- if rel is not none -%}
                {%- do staging_models.append(row['model_name']) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

{%- else -%}
    {# --- New Mode --- #}
    {%- set master_model_id = arg1 -%}

    {%- if execute -%}
        {%- for node in graph.nodes.values() -%}
            {%- if node.resource_type == 'model' and node.config.meta.get('master_model_id') == master_model_id -%}
                {%- do staging_models.append(node.name) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

{%- endif -%}

WITH union_base AS (
    {%- if staging_models | length > 0 -%}
        {%- for stg_ref in staging_models %}
            SELECT
                tenant_slug,
                tenant_skey,
                source_platform,
                source_schema_hash,
                source_schema,
                raw_data_payload
            FROM {{ ref(stg_ref) }}
            {%- if not loop.last %} UNION ALL {% endif -%}
        {%- endfor -%}
    {%- else -%}
        SELECT
            CAST(NULL AS VARCHAR) as tenant_slug,
            CAST(NULL AS VARCHAR) as tenant_skey,
            CAST(NULL AS VARCHAR) as source_platform,
            CAST(NULL AS VARCHAR) as source_schema_hash,
            CAST(NULL AS JSON) as source_schema,
            CAST(NULL AS JSON) as raw_data_payload
        WHERE 1=0
    {%- endif -%}
)
SELECT * FROM union_base
{% endmacro %}