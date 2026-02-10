-- Intermediate: Wayne Enterprises Bing Ads Ad Groups
{{ config(materialized='view') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    raw_data_payload->>'$.Id'           AS ad_group_id,
    raw_data_payload->>'$.CampaignId'   AS campaign_id,
    raw_data_payload->>'$.Name'         AS ad_group_name,

    raw_data_payload

FROM {{ ref('platform_mm__bing_ads_api_v1_ad_groups') }}
WHERE tenant_slug = 'wayne_enterprises'
  AND source_platform = 'bing_ads'
