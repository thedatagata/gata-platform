{% macro extract_field(field_name, target_type='VARCHAR', default_value=NONE, should_alias=True) %}
    COALESCE(CAST(raw_data_payload->>'$.{{ field_name }}' AS {{ target_type }}), {{ default_value if default_value is not none else 'NULL' }})
    {%- if should_alias %} as {{ field_name }} {%- endif -%}
{% endmacro %}
