-- Intermediate: Tyrell Corp Facebook Ads
{{ config(materialized='view') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    raw_data_payload->>'$.id'            AS ad_id,
    raw_data_payload->>'$.adset_id'      AS adset_id,
    raw_data_payload->>'$.campaign_id'   AS campaign_id,
    raw_data_payload->>'$.name'          AS ad_name,
    raw_data_payload->>'$.creative_id'   AS creative_id,
    raw_data_payload->>'$.status'        AS status,

    raw_data_payload

FROM {{ ref('platform_mm__facebook_ads_api_v1_ads') }}
WHERE tenant_slug = 'tyrell_corp'
  AND source_platform = 'facebook_ads'
