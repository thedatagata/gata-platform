WITH facts AS (
    SELECT * FROM {{ ref('int_unified_ad_performance') }}
),

campaigns AS (
    SELECT * FROM {{ ref('int_unified_campaigns') }}
)

SELECT
    f.tenant_slug,
    f.source_platform,
    f.date_start,
    f.campaign_key,
    c.name as campaign_name,
    c.status as campaign_status,
    c.objective as campaign_objective,
    -- Metrics
    f.spend,
    f.impressions,
    f.clicks,
    -- Custom calculations injected via tenant logic? (Already in int?)
    -- Actually, apply_tenant_logic 'calculation' injections usually go into the hydrator or here.
    -- The user example showed apply_tenant_logic at the end of the query.
    -- If 'calculations' add columns like 'cpm', they need to be in the SELECT list.
    -- My apply_tenant_logic macro adds ", formula as alias".
    -- So I should add it to int_unified_ad_performance SELECT list to be safe.
    f.loaded_at
FROM facts f
LEFT JOIN campaigns c ON f.campaign_key = c.entity_key
