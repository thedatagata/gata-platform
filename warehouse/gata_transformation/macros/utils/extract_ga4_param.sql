{% macro extract_ga4_param(param_key, should_alias=True) %}
    {# 
       Extracts a value from the GA4 event_params array of structs.
    #}
    COALESCE(
        (list_filter(
            from_json(raw_data_payload->>'$.event_params', 'JSON[]'), 
            x -> x->>'$.key' = '{{ param_key }}'
        )[1]->'value'->>'$.string_value'),
        CAST((list_filter(
            from_json(raw_data_payload->>'$.event_params', 'JSON[]'), 
            x -> x->>'$.key' = '{{ param_key }}'
        )[1]->'value'->>'$.int_value') AS VARCHAR),
        CAST((list_filter(
            from_json(raw_data_payload->>'$.event_params', 'JSON[]'), 
            x -> x->>'$.key' = '{{ param_key }}'
        )[1]->'value'->>'$.double_value') AS VARCHAR)
    )
    {%- if should_alias %} as {{ param_key }} {%- endif -%}
{% endmacro %}
