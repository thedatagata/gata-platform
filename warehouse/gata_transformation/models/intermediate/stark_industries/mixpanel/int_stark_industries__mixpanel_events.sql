-- Intermediate: Stark Industries Mixpanel Events
-- Source: platform_mm__mixpanel_api_v1_events
-- Note: Generator flattens properties.* to prop_* at top level
-- Note: prop_time is Unix seconds — multiply by 1000 for millisecond epoch
{{ config(materialized='view') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    -- Event core
    raw_data_payload->>'$.event'                                   AS event_name,
    -- Convert seconds → milliseconds for engine sessionization (30-min gap = 1,800,000 ms)
    CAST(raw_data_payload->>'$.prop_time' AS BIGINT) * 1000       AS event_timestamp,

    -- User identifiers
    raw_data_payload->>'$.prop_distinct_id'                        AS user_pseudo_id,
    CAST(NULL AS VARCHAR)                                          AS user_id,

    -- Traffic attribution (UTM params)
    raw_data_payload->>'$.prop_utm_source'                         AS traffic_source,
    raw_data_payload->>'$.prop_utm_medium'                         AS traffic_medium,
    raw_data_payload->>'$.prop_utm_campaign'                       AS traffic_campaign,

    -- Geo
    raw_data_payload->>'$.prop_country_code'                       AS geo_country,

    -- Device
    raw_data_payload->>'$.prop_device_type'                        AS device_category,

    raw_data_payload

FROM {{ ref('platform_mm__mixpanel_api_v1_events') }}
WHERE tenant_slug = 'stark_industries'
  AND source_platform = 'mixpanel'
