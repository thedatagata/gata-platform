WITH users AS (
    SELECT * FROM {{ ref('int_unified_users') }}
),
resolution AS (
    SELECT * FROM {{ ref('int_identity_resolution') }}
),
ltv AS (
    SELECT 
        resolved_user_id,
        SUM(total_price) as total_lifetime_spend,
        COUNT(order_id) as total_order_count
    FROM {{ ref('fct_unified_orders') }}
    WHERE resolved_user_id IS NOT NULL
    GROUP BY 1
)
SELECT
    u.*,
    -- Identity Links
    r.user_pseudo_id as cookie_id,
    r.resolved_at as first_seen_at, -- Assuming we want this too
    
    -- Behavioral Metrics (LTV)
    COALESCE(l.total_lifetime_spend, 0) as total_lifetime_spend,
    COALESCE(l.total_order_count, 0) as total_order_count
    
FROM users u
LEFT JOIN resolution r ON u.source_user_id = r.resolved_user_id
LEFT JOIN ltv l ON u.source_user_id = l.resolved_user_id

QUALIFY ROW_NUMBER() OVER (PARTITION BY user_key ORDER BY loaded_at DESC) = 1
