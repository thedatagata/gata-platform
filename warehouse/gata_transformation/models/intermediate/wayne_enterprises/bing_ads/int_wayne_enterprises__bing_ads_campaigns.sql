-- Intermediate: Wayne Enterprises Bing Ads Campaigns
{{ config(materialized='view') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    raw_data_payload->>'$.Id'       AS campaign_id,
    raw_data_payload->>'$.Name'     AS campaign_name,
    raw_data_payload->>'$.Status'   AS status,

    raw_data_payload

FROM {{ ref('platform_mm__bing_ads_api_v1_campaigns') }}
WHERE tenant_slug = 'wayne_enterprises'
  AND source_platform = 'bing_ads'
