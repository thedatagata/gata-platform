{#
  Funnel Analysis Utility Macros

  These macros support funnel analysis in the sessions fact table.
  get_funnel_events() is the single source of truth in dbt for the funnel definition.
  It mirrors FUNNEL_EVENTS in services/mock-data-engine/sources/digital_analytics/shared_config.py
  — keep them in sync.

  The other 3 macros call get_funnel_events() internally so engines don't pass anything in.
#}


{% macro get_funnel_events() %}
    {{- return([
        'session_start',
        'view_item',
        'add_to_cart',
        'begin_checkout',
        'add_payment_info',
        'purchase'
    ]) -}}
{% endmacro %}


{% macro check_funnel_position(event_name_col) %}
    {%- set funnel_events = get_funnel_events() -%}
    CASE
        {%- for event_name in funnel_events %}
        WHEN {{ event_name_col }} = '{{ event_name }}' THEN {{ loop.index }}
        {%- endfor %}
        ELSE 0
    END
{% endmacro %}


{% macro build_funnel_pivot_columns() %}
    {%- set funnel_events = get_funnel_events() -%}
    {%- for event_name in funnel_events %}
    {% if not loop.first %}, {% endif %}SUM(CASE WHEN event_name = '{{ event_name }}' THEN 1 ELSE 0 END) AS funnel_step_{{ loop.index }}_{{ event_name }}
    {%- endfor %}
{% endmacro %}


{% macro build_funnel_max_step() %}
    {%- set funnel_events = get_funnel_events() -%}
    MAX(
        CASE
            {%- for event_name in funnel_events %}
            WHEN event_name = '{{ event_name }}' THEN {{ loop.index }}
            {%- endfor %}
            ELSE 0
        END
    )
{% endmacro %}
