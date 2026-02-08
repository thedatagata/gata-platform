{{ config(materialized='table') }}

WITH events AS (
    SELECT * FROM {{ ref('int_unified_events') }}
    WHERE user_id IS NOT NULL 
      AND user_pseudo_id IS NOT NULL
      AND user_id != ''
      AND user_pseudo_id != ''
)

SELECT
    tenant_slug,
    user_pseudo_id,
    -- ARG_MIN logic: Get the first user_id associated with this cookie
    arg_min(user_id, event_timestamp) as resolved_user_id,
    min(event_timestamp) as resolved_at
FROM events
GROUP BY 1, 2
