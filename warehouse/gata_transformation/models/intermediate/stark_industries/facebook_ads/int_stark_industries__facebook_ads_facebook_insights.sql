-- Intermediate: Stark Industries Facebook Ads Insights
-- Source: platform_mm__facebook_ads_api_v1_facebook_insights
-- Logic: attribution_model = 'last_click' (from tenants.yaml)
{{ config(materialized='view') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    -- Dimensions
    CAST(raw_data_payload->>'$.date_start' AS DATE)       AS report_date,
    raw_data_payload->>'$.campaign_id'                     AS campaign_id,
    raw_data_payload->>'$.adset_id'                        AS adset_id,
    raw_data_payload->>'$.ad_id'                           AS ad_id,

    -- Metrics
    CAST(raw_data_payload->>'$.spend' AS DOUBLE)           AS spend,
    CAST(raw_data_payload->>'$.impressions' AS BIGINT)     AS impressions,
    CAST(raw_data_payload->>'$.clicks' AS BIGINT)          AS clicks,
    CAST(raw_data_payload->>'$.conversions' AS BIGINT)     AS conversions,
    CAST(raw_data_payload->>'$.cpc' AS DOUBLE)             AS cpc,
    CAST(raw_data_payload->>'$.cpm' AS DOUBLE)             AS cpm,
    CAST(raw_data_payload->>'$.ctr' AS DOUBLE)             AS ctr,

    raw_data_payload

FROM {{ ref('platform_mm__facebook_ads_api_v1_facebook_insights') }}
WHERE tenant_slug = 'stark_industries'
  AND source_platform = 'facebook_ads'
  AND CAST(raw_data_payload->>'$.spend' AS DOUBLE) > 0
