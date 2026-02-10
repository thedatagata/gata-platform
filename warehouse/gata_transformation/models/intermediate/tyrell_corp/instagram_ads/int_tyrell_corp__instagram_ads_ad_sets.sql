-- Intermediate: Tyrell Corp Instagram Ads Ad Sets
{{ config(materialized='view') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    raw_data_payload->>'$.id'            AS adset_id,
    raw_data_payload->>'$.campaign_id'   AS campaign_id,
    raw_data_payload->>'$.name'          AS adset_name,
    raw_data_payload->>'$.status'        AS status,
    CAST(raw_data_payload->>'$.daily_budget' AS DOUBLE) AS daily_budget,

    raw_data_payload

FROM {{ ref('platform_mm__facebook_ads_api_v1_ad_sets') }}
WHERE tenant_slug = 'tyrell_corp'
  AND source_platform = 'instagram_ads'
