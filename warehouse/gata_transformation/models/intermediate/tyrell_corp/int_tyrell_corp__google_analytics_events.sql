{{ config(materialized='table') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    raw_data_payload->>'$.event_name'                            AS event_name,
    CAST(raw_data_payload->>'$.event_timestamp' AS BIGINT)       AS event_timestamp,
    raw_data_payload->>'$.user_pseudo_id'                        AS user_pseudo_id,
    raw_data_payload->>'$.user_id'                               AS user_id,
    raw_data_payload->>'$.traffic_source_source'                 AS traffic_source,
    raw_data_payload->>'$.traffic_source_medium'                 AS traffic_medium,
    raw_data_payload->>'$.traffic_source_campaign'               AS traffic_campaign,
    raw_data_payload->>'$.geo_country'                           AS geo_country,
    raw_data_payload->>'$.device_category'                       AS device_category,
    CAST(raw_data_payload->>'$.ecommerce_value' AS DOUBLE)       AS purchase_revenue,
    raw_data_payload->>'$.ecommerce_transaction_id'              AS transaction_id,

    raw_data_payload

FROM {{ ref('platform_mm__google_analytics_api_v1_events') }}
WHERE tenant_slug = 'tyrell_corp'
  AND source_platform = 'google_analytics'
