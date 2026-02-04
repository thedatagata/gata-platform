{% macro engine_ga_funnel(tenant_slug) %}
{#
    Engine: Google Analytics Funnel (Config-driven, event-based)
    Reads conversion_events from tenant manifest logic config.
    
    Note: Mock data lacks page_location, so funnel steps are event-based
    rather than URL-pattern-based. Tenant config should define funnel_events
    (ordered list of event names) instead of funnel_steps (URL patterns).
#}
{%- set config = get_tenant_config(tenant_slug) -%}
{%- set ga_config = config.get('sources', {}).get('google_analytics', {}).get('logic', {}) if config else {} -%}
{%- set funnel_events = ga_config.get('funnel_events', {}) -%}
{%- set conversion_events = ga_config.get('conversion_events', ['purchase']) -%}

WITH ga AS (
    {{ engine_google_analytics(tenant_slug, ga_config) }}
)

SELECT
    date,
    session_source as source,
    session_medium as medium,
    session_campaign as campaign,
    user_pseudo_id,
    ga_session_id,
    event_name,
    event_timestamp,
    transaction_id,
    transaction_value,

    {# Dynamic funnel step flags (event-name based) #}
    {% for col_name, event_pattern in funnel_events.items() %}
    CASE WHEN event_name = '{{ event_pattern }}' THEN ga_session_id END AS {{ col_name }},
    {% endfor %}

    {# Conversion detection #}
    CASE WHEN event_name IN (
        {% for e in conversion_events %}'{{ e }}'{% if not loop.last %}, {% endif %}{% endfor %}
    ) THEN 1 ELSE 0 END as is_conversion
FROM ga
{% endmacro %}
