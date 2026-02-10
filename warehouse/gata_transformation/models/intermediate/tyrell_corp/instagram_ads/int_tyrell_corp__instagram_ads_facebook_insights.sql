-- Intermediate: Tyrell Corp Instagram Ads Insights
-- Shares master model with Facebook Ads, filtered by source_platform
{{ config(materialized='view') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    CAST(raw_data_payload->>'$.date_start' AS DATE)       AS report_date,
    raw_data_payload->>'$.campaign_id'                     AS campaign_id,
    raw_data_payload->>'$.adset_id'                        AS adset_id,
    raw_data_payload->>'$.ad_id'                           AS ad_id,

    CAST(raw_data_payload->>'$.spend' AS DOUBLE)           AS spend,
    CAST(raw_data_payload->>'$.impressions' AS BIGINT)     AS impressions,
    CAST(raw_data_payload->>'$.clicks' AS BIGINT)          AS clicks,
    CAST(raw_data_payload->>'$.conversions' AS BIGINT)     AS conversions,
    CAST(raw_data_payload->>'$.cpc' AS DOUBLE)             AS cpc,
    CAST(raw_data_payload->>'$.cpm' AS DOUBLE)             AS cpm,
    CAST(raw_data_payload->>'$.ctr' AS DOUBLE)             AS ctr,

    raw_data_payload

FROM {{ ref('platform_mm__facebook_ads_api_v1_facebook_insights') }}
WHERE tenant_slug = 'tyrell_corp'
  AND source_platform = 'instagram_ads'
  AND CAST(raw_data_payload->>'$.spend' AS DOUBLE) > 0
