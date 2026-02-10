-- Intermediate: Tyrell Corp Google Ads
{{ config(materialized='view') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    raw_data_payload->>'$.resource_name'  AS resource_name,
    raw_data_payload->>'$.id'             AS ad_id,
    raw_data_payload->>'$.ad_group_id'    AS ad_group_id,
    raw_data_payload->>'$.name'           AS ad_name,
    raw_data_payload->>'$.status'         AS status,

    raw_data_payload

FROM {{ ref('platform_mm__google_ads_api_v1_ads') }}
WHERE tenant_slug = 'tyrell_corp'
  AND source_platform = 'google_ads'
