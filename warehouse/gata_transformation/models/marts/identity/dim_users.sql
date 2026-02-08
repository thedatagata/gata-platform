WITH users AS (
    SELECT * FROM {{ ref('int_unified_users') }}
)
SELECT
    *
FROM users
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_key ORDER BY loaded_at DESC) = 1
