WITH events AS (
    SELECT * FROM {{ ref('int_unified_events') }}
)
SELECT
    *,
    -- Standardize event names if needed (e.g. page_view vs pageview)
    LOWER(event_name) as standardized_event_name
FROM events
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_key ORDER BY loaded_at DESC) = 1
