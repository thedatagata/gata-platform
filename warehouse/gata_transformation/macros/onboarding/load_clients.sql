{% macro get_manifest() %}
-- Dynamically load the project-level tenants.yaml
{% set manifest_path = '../../tenants.yaml' %}
{% set manifest_yaml = '' %}
{% if execute %}
    {% set query %}
        select content from read_text('{{ manifest_path }}')
    {% endset %}
    {% set results = run_query(query) %}
    {% set manifest_yaml = results.columns[0].values()[0] %}
{% endif %}
{{ return(fromyaml(manifest_yaml)) }}
{% endmacro %}

{% macro get_all_clients() %}
{%- set manifest = get_manifest() -%}
{{ return(manifest.tenants) }}
{% endmacro %}