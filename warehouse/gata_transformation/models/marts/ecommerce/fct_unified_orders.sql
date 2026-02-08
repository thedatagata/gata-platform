WITH orders AS (
    SELECT * FROM {{ ref('int_unified_orders') }}
),
users AS (
    SELECT * FROM {{ ref('dim_users') }}
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
    
    -- Attribution Placeholder (simplistic)
    -- If we had a session_id on the order, we'd join fct_sessions.
    -- For now, exposing landing_site for potential regex parsing.
    o.landing_site,
    
    o.loaded_at
FROM orders o
LEFT JOIN users u 
    ON o.tenant_slug = u.tenant_slug 
    AND (o.email = u.email OR o.customer_id = u.source_user_id)
