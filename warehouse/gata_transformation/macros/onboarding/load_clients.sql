{% macro get_manifest() %}
-- Dynamically load the project-level tenants.yaml
{% set manifest_yaml = '' %}
{% if execute %}
    {% set query %}
        select file from glob(['./tenants.yaml', '../../tenants.yaml']) limit 1
    {% endset %}
    {% set path_results = run_query(query) %}
    
    {% if path_results|length > 0 %}
        {% set manifest_path = path_results.columns[0].values()[0] %}
        {% set read_query %}
            select content from read_text('{{ manifest_path }}')
        {% endset %}
        {% set results = run_query(read_query) %}
        {% set manifest_yaml = results.columns[0].values()[0] %}
    {% endif %}
{% endif %}
{{ return(fromyaml(manifest_yaml)) }}
{% endmacro %}

{% macro get_all_clients() %}
{%- set manifest = get_manifest() -%}
{{ return(manifest.tenants) }}
{% endmacro %}