{%- macro clean_env_string(val) -%}
    {%- if val is none -%}
        null
    {%- else -%}
        {{ val | replace("\\", "\\\\") | replace('"""', '\\"\\"\\"') | replace("\n", " ") | trim }}
    {%- endif -%}
{%- endmacro -%}

{% macro clean_string(column_name) %}
    regexp_replace(CAST({{ column_name }} AS VARCHAR), '[^\x00-\x7F]+', '', 'g')
{% endmacro %}
