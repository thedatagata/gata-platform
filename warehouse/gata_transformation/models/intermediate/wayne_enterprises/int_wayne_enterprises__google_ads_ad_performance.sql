{{ config(materialized='table') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    CAST(raw_data_payload->>'$.date_start' AS DATE) AS report_date,
    CAST(raw_data_payload->>'$.cost_micros' AS BIGINT) / 1000000.0 AS spend,
    CAST(raw_data_payload->>'$.impressions' AS BIGINT) AS impressions,
    CAST(raw_data_payload->>'$.clicks' AS BIGINT) AS clicks,
    CAST(raw_data_payload->>'$.conversions' AS DOUBLE) AS conversions,
    raw_data_payload->>'$.campaign_id' AS campaign_id,
    raw_data_payload->>'$.ad_group_id' AS ad_group_id,
    raw_data_payload->>'$.ad_id' AS ad_id,

    raw_data_payload

FROM {{ ref('platform_mm__google_ads_api_v1_ad_performance') }}
WHERE tenant_slug = 'wayne_enterprises'
  AND source_platform = 'google_ads'