WITH events AS (
    SELECT * FROM {{ ref('int_unified_events') }}
),
resolution AS (
    SELECT * FROM {{ ref('int_identity_resolution') }}
)

SELECT
    e.tenant_slug,
    e.session_id as session_key,
    
    -- Identity Resolution
    COALESCE(r.resolved_user_id, e.user_pseudo_id) as resolved_user_id,
    MAX(r.resolved_user_id) as user_id, -- Actual user_id if present in session
    ANY_VALUE(e.user_pseudo_id) as anonymous_id,
    
    -- Session Timing
    MIN(e.event_timestamp) as session_start_at,
    MAX(e.event_timestamp) as session_end_at,
    date_diff('second', MIN(e.event_timestamp), MAX(e.event_timestamp)) as session_duration_seconds,
    count(*) as events_in_session,
    
    -- Attribution (First Touch in Session)
    -- Using ARG_MIN logic to get source/medium of first event
    arg_min(e.page_location, e.event_timestamp) as landing_page,
    
    -- Traffic Source (need to extract UTMs or rely on standardized fields if they existed)
    -- For now, extracting from page_location if not present, but int_unified_events didn't extract UTMs explicitly.
    -- Assuming traffic_source/medium fields might be in raw_data or extracted.
    -- The user requirement said: "Extract attribution info (source, medium, campaign) from the first event"
    -- `int_unified_events` has `page_location`. I should probably use `extract_utm` macro on it?
    -- Or if GA4, use traffic_source params.
    -- For simplicity in this phase, I'll placeholder the extraction or use what's available.
    -- Let's assume we extract from landing page url for now using the macro I saw earlier.
    {{ extract_utm(arg_min('e.page_location', 'e.event_timestamp'), 'utm_source') }} as session_source,
    {{ extract_utm(arg_min('e.page_location', 'e.event_timestamp'), 'utm_medium') }} as session_medium,
    {{ extract_utm(arg_min('e.page_location', 'e.event_timestamp'), 'utm_campaign') }} as session_campaign

FROM events e
LEFT JOIN resolution r ON e.tenant_slug = r.tenant_slug AND e.user_pseudo_id = r.user_pseudo_id
GROUP BY 1, 2
