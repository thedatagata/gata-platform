-- Intermediate: Wayne Enterprises Google Ads Customers
{{ config(materialized='view') }}

SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,

    raw_data_payload->>'$.resource_name'     AS resource_name,
    raw_data_payload->>'$.id'                AS customer_id,
    raw_data_payload->>'$.descriptive_name'  AS customer_name,
    raw_data_payload->>'$.currency_code'     AS currency_code,
    raw_data_payload->>'$.time_zone'         AS time_zone,

    raw_data_payload

FROM {{ ref('platform_mm__google_ads_api_v1_customers') }}
WHERE tenant_slug = 'wayne_enterprises'
  AND source_platform = 'google_ads'
