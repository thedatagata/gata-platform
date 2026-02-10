-- Intermediate: Wayne Enterprises Google Ads Campaigns
{{ config(materialized='view') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    raw_data_payload->>'$.resource_name'              AS resource_name,
    raw_data_payload->>'$.id'                         AS campaign_id,
    raw_data_payload->>'$.name'                       AS campaign_name,
    raw_data_payload->>'$.status'                     AS status,
    raw_data_payload->>'$.advertising_channel_type'   AS channel_type,

    raw_data_payload

FROM {{ ref('platform_mm__google_ads_api_v1_campaigns') }}
WHERE tenant_slug = 'wayne_enterprises'
  AND source_platform = 'google_ads'
