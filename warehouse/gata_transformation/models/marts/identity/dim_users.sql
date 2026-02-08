WITH users AS (
    SELECT * FROM {{ ref('int_unified_users') }}
),
identity_map AS (
    SELECT * FROM {{ ref('int_identity_resolution') }}
),
orders AS (
    SELECT
        resolved_user_id,
        count(*) as total_orders,
        sum(total_price) as total_spend
    FROM {{ ref('fct_unified_orders') }}
    WHERE resolved_user_id IS NOT NULL
    GROUP BY 1
)

SELECT
    u.tenant_slug,
    u.source_platform,
    u.user_key,
    u.source_user_id,
    u.email,
    u.created_at,
    
    -- Identity Links
    im.resolved_user_id as mapped_user_id, -- Link to unified ID if different
    
    -- Behavioral Metrics (LTV)
    COALESCE(o.total_orders, 0) as lifetime_orders,
    COALESCE(o.total_spend, 0) as lifetime_spend,
    
    u.loaded_at
FROM users u
LEFT JOIN identity_map im 
    ON u.tenant_slug = im.tenant_slug 
    AND u.source_user_id = im.resolved_user_id -- Assuming source_user_id IS the resolved one for registered users
LEFT JOIN orders o
    ON u.source_user_id = o.resolved_user_id

QUALIFY ROW_NUMBER() OVER (PARTITION BY u.user_key ORDER BY u.loaded_at DESC) = 1
