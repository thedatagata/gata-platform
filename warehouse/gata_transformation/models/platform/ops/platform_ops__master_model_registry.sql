{{ config(materialized='table') }}

-- Mapping known schema hashes to Master Model identifiers
SELECT 
    source_schema_hash,
    platform_name as source_platform,
    source_table_name,
    CASE 
        WHEN platform_name IN ('facebook_ads', 'instagram_ads') THEN 
            CASE 
                WHEN source_table_name LIKE '%campaigns' THEN 'facebook_ads_api_v1_campaigns'
                WHEN source_table_name LIKE '%ad_sets' THEN 'facebook_ads_api_v1_ad_sets'
                WHEN source_table_name LIKE '%ads' THEN 'facebook_ads_api_v1_ads'
                WHEN source_table_name LIKE '%facebook_insights' THEN 'facebook_ads_api_v1_facebook_insights'
                ELSE 'facebook_ads_api_v1_unknown'
            END
        WHEN platform_name = 'google_ads' THEN 
             CASE 
                WHEN source_table_name LIKE '%campaigns' THEN 'google_ads_api_v1_campaigns'
                WHEN source_table_name LIKE '%ad_groups' THEN 'google_ads_api_v1_ad_groups'
                WHEN source_table_name LIKE '%ads' THEN 'google_ads_api_v1_ads'
                WHEN source_table_name LIKE '%customers' THEN 'google_ads_api_v1_customers'
                WHEN source_table_name LIKE '%ad_performance' THEN 'google_ads_api_v1_ad_performance'
                ELSE 'google_ads_api_v1_unknown'
            END
        WHEN platform_name = 'google_analytics' THEN 
            CASE 
                WHEN source_table_name LIKE '%events' THEN 'google_analytics_api_v1_events'
                ELSE 'google_analytics_api_v1_unknown'
            END
        WHEN platform_name = 'shopify' THEN 
            CASE 
                WHEN source_table_name LIKE '%products' THEN 'shopify_api_v1_products'
                WHEN source_table_name LIKE '%orders' THEN 'shopify_api_v1_orders'
                ELSE 'shopify_api_v1_unknown'
            END
        WHEN platform_name = 'stripe' THEN 
            CASE 
                WHEN source_table_name LIKE '%charges' THEN 'stripe_api_v1_charges'
                ELSE 'stripe_api_v1_unknown'
            END
        WHEN platform_name = 'woocommerce' THEN 
            CASE 
                WHEN source_table_name LIKE '%orders' THEN 'woocommerce_api_v1_orders'
                WHEN source_table_name LIKE '%products' THEN 'woocommerce_api_v1_products'
                ELSE 'woocommerce_api_v1_unknown'
            END
        WHEN platform_name = 'linkedin_ads' THEN 
            CASE 
                WHEN source_table_name LIKE '%campaigns' THEN 'linkedin_ads_api_v1_campaigns'
                WHEN source_table_name LIKE '%ad_analytics' THEN 'linkedin_ads_api_v1_ad_analytics'
                ELSE 'linkedin_ads_api_v1_unknown'
            END
        ELSE 'unmapped'
    END as master_model_id
FROM {{ ref('platform_sat__source_schema_history') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY source_schema_hash ORDER BY updated_at DESC) = 1

UNION ALL

SELECT
    source_schema_hash,
    source_name as source_platform,
    source_table_name,
    master_model_id
FROM connectors.main.connector_blueprints
