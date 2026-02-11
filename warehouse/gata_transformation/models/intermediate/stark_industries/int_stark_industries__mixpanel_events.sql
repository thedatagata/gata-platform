{{ config(materialized='table') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    raw_data_payload->>'$.event'                                 AS event_name,
    CAST(raw_data_payload->>'$.prop_time' AS BIGINT) * 1000      AS event_timestamp,
    raw_data_payload->>'$.prop_distinct_id'                      AS user_pseudo_id,
    CAST(NULL AS VARCHAR)                                        AS user_id,
    raw_data_payload->>'$.prop_utm_source'                       AS traffic_source,
    raw_data_payload->>'$.prop_utm_medium'                       AS traffic_medium,
    raw_data_payload->>'$.prop_utm_campaign'                     AS traffic_campaign,
    raw_data_payload->>'$.prop_country_code'                     AS geo_country,
    raw_data_payload->>'$.prop_device_type'                      AS device_category,

    raw_data_payload

FROM {{ ref('platform_mm__mixpanel_api_v1_events') }}
WHERE tenant_slug = 'stark_industries'
  AND source_platform = 'mixpanel'
