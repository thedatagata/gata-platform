WITH campaigns AS (
    SELECT * FROM {{ ref('int_unified_campaigns') }}
),

performance AS (
    SELECT 
        campaign_key,
        SUM(COALESCE(spend, 0)) as total_spend,
        SUM(COALESCE(impressions, 0)) as total_impressions,
        SUM(COALESCE(clicks, 0)) as total_clicks
    FROM {{ ref('fct_unified_ad_performance') }}
    GROUP BY 1
)

SELECT
    c.tenant_slug,
    c.source_platform,
    c.entity_key as campaign_key,
    c.name as campaign_name,
    c.status as campaign_status,
    c.objective,
    
    -- Aggregated Metrics
    COALESCE(p.total_spend, 0) as lifetime_spend,
    COALESCE(p.total_impressions, 0) as lifetime_impressions,
    COALESCE(p.total_clicks, 0) as lifetime_clicks,
    
    c.loaded_at
FROM campaigns c
LEFT JOIN performance p ON c.entity_key = p.campaign_key
