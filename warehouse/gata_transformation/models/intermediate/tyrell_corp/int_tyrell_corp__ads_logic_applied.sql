-- gata_transformation/models/intermediate/tyrell_corp/int_tyrell_corp__ads_logic_applied.sql

WITH master_data AS (
    -- Fetch raw data from the thin Master Model
    SELECT * FROM {{ ref('platform_mm__facebook_ads_api_v1_facebook_insights') }}
    WHERE hub_key = '{{ generate_tenant_key("tyrell_corp") }}'
),

current_logic AS (
    -- Fetch the specific logic block for this table
    SELECT * FROM {{ ref('platform_sat__tenant_config_history') }}
    WHERE tenant_slug = 'tyrell_corp'
      AND source_name = 'facebook_ads'
      AND table_name = 'facebook_insights'
    ORDER BY updated_at DESC
    LIMIT 1
)

SELECT 
    m.*,
    c.table_logic,
    c.logic_hash
FROM master_data m
CROSS JOIN current_logic c -- Dynamically applies latest logic to all historical rows
