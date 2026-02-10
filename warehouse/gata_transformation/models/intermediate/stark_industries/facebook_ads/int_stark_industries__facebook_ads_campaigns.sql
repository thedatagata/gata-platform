-- Intermediate: Stark Industries Facebook Ads Campaigns
{{ config(materialized='view') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    raw_data_payload->>'$.id'         AS campaign_id,
    raw_data_payload->>'$.name'       AS campaign_name,
    raw_data_payload->>'$.objective'  AS objective,
    raw_data_payload->>'$.status'     AS status,

    raw_data_payload

FROM {{ ref('platform_mm__facebook_ads_api_v1_campaigns') }}
WHERE tenant_slug = 'stark_industries'
  AND source_platform = 'facebook_ads'
