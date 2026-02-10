-- Intermediate: Wayne Enterprises Google Analytics Events
{{ config(materialized='view') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    raw_data_payload->>'$.event_date'                              AS event_date,
    CAST(raw_data_payload->>'$.event_timestamp' AS BIGINT)        AS event_timestamp,
    raw_data_payload->>'$.event_name'                              AS event_name,
    raw_data_payload->>'$.user_pseudo_id'                          AS user_pseudo_id,
    raw_data_payload->>'$.user_id'                                 AS user_id,

    raw_data_payload->'$.traffic_source'->>'source'                AS traffic_source,
    raw_data_payload->'$.traffic_source'->>'medium'                AS traffic_medium,
    raw_data_payload->'$.traffic_source'->>'campaign'              AS traffic_campaign,

    raw_data_payload->'$.geo'->>'country'                          AS geo_country,
    raw_data_payload->'$.geo'->>'region'                           AS geo_region,
    raw_data_payload->'$.geo'->>'city'                             AS geo_city,

    raw_data_payload->'$.device'->>'category'                      AS device_category,
    raw_data_payload->'$.device'->>'browser'                       AS device_browser,

    raw_data_payload->'$.ecommerce'->>'transaction_id'             AS transaction_id,
    CAST(raw_data_payload->'$.ecommerce'->>'purchase_revenue' AS DOUBLE) AS purchase_revenue,

    raw_data_payload->'$.event_params'                             AS event_params_json,
    raw_data_payload->'$.ecommerce'                                AS ecommerce_json,

    raw_data_payload

FROM {{ ref('platform_mm__google_analytics_api_v1_events') }}
WHERE tenant_slug = 'wayne_enterprises'
  AND source_platform = 'google_analytics'
