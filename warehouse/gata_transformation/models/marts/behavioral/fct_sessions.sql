WITH events AS (
    SELECT * FROM {{ ref('int_unified_events') }}
),
resolution AS (
    SELECT * FROM {{ ref('int_identity_resolution') }}
)

SELECT
    e.tenant_slug,
    -- Fallback Session Key: Use explicit session_id, or generate one from cookie + hour bucketing
    -- This ensures even anonymous "missed" sessions are counted.
    COALESCE(
        e.session_id, 
        md5(concat(e.user_pseudo_id, cast(date_trunc('hour', e.event_timestamp) as varchar)))
    ) as session_key,
    
    -- Identity Resolution
    COALESCE(r.resolved_user_id, e.user_pseudo_id) as resolved_user_id,
    MAX(r.resolved_user_id) as user_id, 
    ANY_VALUE(e.user_pseudo_id) as anonymous_id,
    
    -- Session Timing
    MIN(e.event_timestamp) as session_start_at,
    MAX(e.event_timestamp) as session_end_at,
    date_diff('second', MIN(e.event_timestamp), MAX(e.event_timestamp)) as session_duration_seconds,
    count(*) as events_in_session,
    
    -- Attribution (First Touch in Session)
    arg_min(e.page_location, e.event_timestamp) as landing_page,
    
    COALESCE(
        MAX(e.utm_source), -- If any event in session has source, take it (or use arg_min)
        {{ extract_utm(arg_min('e.page_location', 'e.event_timestamp'), 'utm_source') }}
    ) as session_source,
    
    COALESCE(
        MAX(e.utm_medium),
        {{ extract_utm(arg_min('e.page_location', 'e.event_timestamp'), 'utm_medium') }}
    ) as session_medium,
    
    COALESCE(
        MAX(e.utm_campaign),
        {{ extract_utm(arg_min('e.page_location', 'e.event_timestamp'), 'utm_campaign') }}
    ) as session_campaign

FROM events e
LEFT JOIN resolution r ON e.tenant_slug = r.tenant_slug AND e.user_pseudo_id = r.user_pseudo_id
GROUP BY 1, 2
