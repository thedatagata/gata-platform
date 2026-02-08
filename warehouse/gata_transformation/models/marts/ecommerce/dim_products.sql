WITH products AS (
    SELECT * FROM {{ ref('int_unified_products') }}
)
SELECT
    *
FROM products
QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_key ORDER BY loaded_at DESC) = 1
