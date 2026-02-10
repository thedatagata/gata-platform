-- Intermediate: Wayne Enterprises Google Ads Ad Groups
{{ config(materialized='view') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    raw_data_payload->>'$.resource_name'   AS resource_name,
    raw_data_payload->>'$.id'              AS ad_group_id,
    raw_data_payload->>'$.campaign_id'     AS campaign_id,
    raw_data_payload->>'$.name'            AS ad_group_name,
    raw_data_payload->>'$.status'          AS status,
    raw_data_payload->>'$.type'            AS ad_group_type,

    raw_data_payload

FROM {{ ref('platform_mm__google_ads_api_v1_ad_groups') }}
WHERE tenant_slug = 'wayne_enterprises'
  AND source_platform = 'google_ads'
