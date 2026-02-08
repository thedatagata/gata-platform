WITH orders AS (
    SELECT * FROM {{ ref('int_unified_orders') }}
)
SELECT
    tenant_slug,
    source_platform,
    created_at,
    id as order_id,
    order_number,
    total_price,
    currency,
    status as original_status,
    -- Standardize Status
    CASE 
        WHEN lower(status) IN ('paid', 'completed', 'success') THEN 'SUCCESS'
        WHEN lower(status) IN ('pending', 'authorized') THEN 'PENDING'
        WHEN lower(status) IN ('refunded', 'voided', 'cancelled') THEN 'CANCELLED'
        ELSE 'OTHER'
    END as standardized_status,
    loaded_at
FROM orders
