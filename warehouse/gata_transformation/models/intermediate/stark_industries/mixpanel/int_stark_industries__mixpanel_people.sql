-- Intermediate: Stark Industries Mixpanel People (User Profiles)
{{ config(materialized='view') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    raw_data_payload->>'$.distinct_id'     AS distinct_id,
    raw_data_payload->>'$.city'            AS city,
    raw_data_payload->>'$.email'           AS email,

    raw_data_payload

FROM {{ ref('platform_mm__mixpanel_api_v1_people') }}
WHERE tenant_slug = 'stark_industries'
  AND source_platform = 'mixpanel'
