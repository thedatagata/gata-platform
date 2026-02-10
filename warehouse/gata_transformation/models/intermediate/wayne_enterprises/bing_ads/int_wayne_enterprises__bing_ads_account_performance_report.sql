-- Intermediate: Wayne Enterprises Bing Ads Account Performance
-- Logic: goal = 'lead_gen', target_cpa = 45.0 (from tenants.yaml)
{{ config(materialized='view') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    CAST(raw_data_payload->>'$.TimePeriod' AS DATE)        AS report_date,
    raw_data_payload->>'$.AccountName'                      AS account_name,
    CAST(raw_data_payload->>'$.Spend' AS DOUBLE)           AS spend,
    CAST(raw_data_payload->>'$.Impressions' AS BIGINT)     AS impressions,
    CAST(raw_data_payload->>'$.Clicks' AS BIGINT)          AS clicks,

    raw_data_payload

FROM {{ ref('platform_mm__bing_ads_api_v1_account_performance_report') }}
WHERE tenant_slug = 'wayne_enterprises'
  AND source_platform = 'bing_ads'
