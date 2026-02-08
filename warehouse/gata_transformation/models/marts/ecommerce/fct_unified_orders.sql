WITH orders AS (
    SELECT * FROM {{ ref('int_unified_orders') }}
),
users AS (
    SELECT * FROM {{ ref('dim_users') }}
),
sessions AS (
    SELECT * FROM {{ ref('fct_sessions') }}
)

SELECT
    o.tenant_slug,
    o.source_platform,
    o.created_at,
    o.id as order_id,
    o.order_number,
    o.total_price,
    o.currency,
    o.status as original_status,
    -- Standardize Status
    CASE 
        WHEN lower(o.status) IN ('paid', 'completed', 'success') THEN 'SUCCESS'
        WHEN lower(o.status) IN ('pending', 'authorized') THEN 'PENDING'
        WHEN lower(o.status) IN ('refunded', 'voided', 'cancelled') THEN 'CANCELLED'
        ELSE 'OTHER'
    END as standardized_status,
    
    -- Identity Linkage
    u.user_id as resolved_user_id,
    o.email,
    
    -- Attribution Linkage
    -- Find the most recent session for this user before the order
    s.session_key as attribution_session_key,
    s.session_source as attribution_source,
    s.session_medium as attribution_medium,
    s.session_campaign as attribution_campaign,
    
    o.landing_site, 
    o.loaded_at

FROM orders o
LEFT JOIN users u 
    ON o.tenant_slug = u.tenant_slug 
    AND (o.email = u.email OR o.customer_id = u.source_user_id)

-- Attribution Join: Lateral join or aggressive filtering required for "most recent before"
-- DuckDB supports ASOF joins or we can use a window function approach.
-- For simplicity and standard SQL compatibility in dbt:
LEFT JOIN (
    SELECT 
        s.resolved_user_id,
        s.session_key,
        s.session_source,
        s.session_medium,
        s.session_campaign,
        s.session_start_at,
        s.session_end_at,
        s.tenant_slug
    FROM sessions s
) s ON o.tenant_slug = s.tenant_slug 
   AND u.user_id = s.resolved_user_id
   AND s.session_start_at <= o.created_at
   AND s.session_start_at >= o.created_at - INTERVAL '30 days' -- Attribution Window
   
QUALIFY ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY s.session_start_at DESC) = 1
