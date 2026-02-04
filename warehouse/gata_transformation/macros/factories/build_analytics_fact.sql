{%- macro build_analytics_fact(tenant_slug) -%}
{#
    Factory: Analytics Session Report
    Orchestrates Google Analytics engine with tenant-specific logic config.
    Produces session-level attribution with conversion detection.
    
    Output schema: tenant_slug, date, user_pseudo_id, ga_session_id,
                   session_source, session_medium, session_campaign,
                   event_name, event_timestamp, transaction_id,
                   transaction_value, is_conversion_event
#}
{%- set config = get_tenant_config(tenant_slug) -%}
{%- set sources = config.get('sources', {}) if config else {} -%}
{%- set has_ga = sources.get('google_analytics', {}).get('enabled', false) -%}

{%- if has_ga -%}

{%- set ga_logic = sources.get('google_analytics', {}).get('logic', {}) -%}

WITH ga_sessions AS (
    {{ engine_google_analytics(tenant_slug, ga_logic) }}
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
    raw_data_payload
FROM ga_sessions

{%- else -%}

SELECT 
    '{{ tenant_slug }}' as tenant_slug,
    CAST(NULL AS DATE) as date,
    CAST(NULL AS VARCHAR) as user_pseudo_id,
    CAST(NULL AS VARCHAR) as ga_session_id,
    CAST(NULL AS VARCHAR) as session_source,
    CAST(NULL AS VARCHAR) as session_medium,
    CAST(NULL AS VARCHAR) as session_campaign,
    CAST(NULL AS VARCHAR) as event_name,
    CAST(NULL AS BIGINT) as event_timestamp,
    CAST(NULL AS VARCHAR) as transaction_id,
    CAST(NULL AS DOUBLE) as transaction_value,
    CAST(NULL AS VARCHAR) as transaction_currency,
    CAST(NULL AS BOOLEAN) as is_conversion_event,
    CAST(NULL AS JSON) as raw_data_payload
WHERE 1=0

{%- endif -%}
{%- endmacro -%}
