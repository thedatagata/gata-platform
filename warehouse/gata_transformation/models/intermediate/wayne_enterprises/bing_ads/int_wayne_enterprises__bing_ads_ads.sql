-- Intermediate: Wayne Enterprises Bing Ads
{{ config(materialized='view') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    raw_data_payload->>'$.Id'          AS ad_id,
    raw_data_payload->>'$.AdGroupId'   AS ad_group_id,
    raw_data_payload->>'$.Type'        AS ad_type,
    raw_data_payload->>'$.Title'       AS title,

    raw_data_payload

FROM {{ ref('platform_mm__bing_ads_api_v1_ads') }}
WHERE tenant_slug = 'wayne_enterprises'
  AND source_platform = 'bing_ads'
