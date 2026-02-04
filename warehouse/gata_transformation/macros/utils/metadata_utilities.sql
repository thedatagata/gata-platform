{% macro capture_source_schema(relation) %}
{%- if execute -%}
    {%- set columns = adapter.get_columns_in_relation(relation) -%}
    {%- set schema_dict = {} -%}
    {%- if columns -%}
        {%- for col in columns -%}
            {%- do schema_dict.update({col.name: col.dtype}) -%}
        {%- endfor -%}
        {# Use tojson(sort_keys=True) if available, or manual sort #}
        {# Jinja's dict sorting is not guaranteed, so we extract keys, sort, and rebuild #}
        {%- set sorted_keys = schema_dict.keys() | list | sort -%}
        {%- set sorted_schema = {} -%}
        {%- for key in sorted_keys -%}
            {%- do sorted_schema.update({key: schema_dict[key]}) -%}
        {%- endfor -%}
        '{{ sorted_schema | tojson }}'
    {%- else -%}
        '{"error": "schema_not_captured"}'
    {%- endif -%}
{%- else -%}
    CAST(NULL AS VARCHAR)
{%- endif -%}
{% endmacro %}

{% macro generate_source_schema_hash(relation) %}
{%- if execute -%}
    {# This macro calls capture_source_schema, strips the outer quotes if needed, and hashes it #}
    {# Note: Because capture_source_schema returns a quoted string literal for SQL, key extraction is safer #}
    {%- set schema_json_literal = capture_source_schema(relation) -%}
    {# Clean the literal for hashing - remove single quotes if present #}
    {%- set clean_json = schema_json_literal | replace("'", "") -%}
    SELECT TO_HEX(MD5('{{ clean_json }}'))
{%- else -%}
    CAST(NULL AS VARCHAR)
{%- endif -%}
{% endmacro %}