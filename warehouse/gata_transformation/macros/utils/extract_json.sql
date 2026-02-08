{% macro extract_field(field_name, target_type='VARCHAR', default_value=NONE) %}
    COALESCE(CAST(raw_data_payload->>'$.{{ field_name }}' AS {{ target_type }}), {{ default_value if default_value is not none else 'NULL' }}) as {{ field_name }}
{% endmacro %}
