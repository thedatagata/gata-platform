{#
  Macro: generate_intermediate_unpacker
  Generates intermediate models that extract typed fields from master model raw_data_payload JSON.

  Parameters:
    - tenant_slug: e.g. 'tyrell_corp'
    - source_platform: e.g. 'facebook_ads' (used in WHERE filter)
    - master_model_id: e.g. 'facebook_ads_api_v1_facebook_insights' (maps to platform_mm__{id})
    - columns: list of dicts with {json_key, alias, cast_to}
      Optional keys: json_op ('->' to keep as JSON), expression (raw SQL expression override)

  Usage:
    {{ generate_intermediate_unpacker(
        'tyrell_corp', 'facebook_ads', 'facebook_ads_api_v1_facebook_insights',
        [
            {'json_key': 'date_start', 'alias': 'report_date', 'cast_to': 'DATE'},
            {'json_key': 'spend', 'alias': 'spend', 'cast_to': 'DOUBLE'}
        ]
    ) }}
#}
{% macro generate_intermediate_unpacker(tenant_slug, source_platform, master_model_id, columns) %}

{{ config(materialized='table') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    {%- for col in columns %}
    {%- if col.get('expression') %}
    {{ col.expression }} AS {{ col.alias }},
    {%- elif col.get('json_op') == '->' %}
    raw_data_payload->'$.{{ col.json_key }}' AS {{ col.alias }},
    {%- else %}
    CAST(raw_data_payload->>'$.{{ col.json_key }}' AS {{ col.cast_to }}) AS {{ col.alias }},
    {%- endif %}
    {%- endfor %}

    raw_data_payload

FROM {{ ref('platform_mm__' ~ master_model_id) }}
WHERE tenant_slug = '{{ tenant_slug }}'
  AND source_platform = '{{ source_platform }}'

{% endmacro %}
