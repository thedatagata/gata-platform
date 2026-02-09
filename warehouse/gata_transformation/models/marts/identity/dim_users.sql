WITH users AS (
    SELECT * FROM {{ ref('int_unified_users') }}
),
identity_map AS (
    SELECT 
        resolved_user_id,
        list(user_pseudo_id) as linked_cookies, 
        min(resolved_at) as first_seen_at
    FROM {{ ref('int_identity_resolution') }}
    GROUP BY 1
),
orders AS (
    SELECT
        resolved_user_id, -- Maps to u.source_user_id
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
    im.linked_cookies,
    im.first_seen_at,
    
    -- Behavioral Metrics (LTV)
    COALESCE(o.total_orders, 0) as lifetime_orders,
    COALESCE(o.total_spend, 0) as lifetime_spend,
    
    u.loaded_at
FROM users u
LEFT JOIN identity_map im 
    ON u.tenant_slug = im.tenant_slug 
    AND u.source_user_id = im.resolved_user_id 
LEFT JOIN orders o
    ON u.source_user_id = o.resolved_user_id

QUALIFY ROW_NUMBER() OVER (PARTITION BY u.user_key ORDER BY u.loaded_at DESC) = 1
