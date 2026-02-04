{% macro engine_ga_attribution(tenant_slug, logic_config={}) %}
{#
    Engine: Google Analytics Session Attribution
    Input: Calls engine_google_analytics for base extraction + session construction
    
    Produces session-level attribution with traffic source,
    conversion detection, and ecommerce value.
#}
{%- set config = get_tenant_config(tenant_slug) if not logic_config else logic_config -%}
{%- set ga_config = config.get('sources', {}).get('google_analytics', {}).get('logic', {}) if not logic_config else logic_config -%}

WITH ga AS (
    {{ engine_google_analytics(tenant_slug, ga_config) }}
)

SELECT
    tenant_slug,
    date,
    user_pseudo_id,
    ga_session_id,
    session_source,
    session_medium,
    session_campaign,
    event_name,
    event_timestamp,
    transaction_id,
    transaction_value,
    transaction_currency,
    is_conversion_event,
    ROW_NUMBER() OVER (
        PARTITION BY user_pseudo_id, ga_session_id
        ORDER BY event_timestamp
    ) AS event_rank
FROM ga
{% endmacro %}
