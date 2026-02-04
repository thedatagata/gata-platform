WITH raw_source AS (
    SELECT *
    FROM {{ source('tyrell_corp_shopify_raw', 'raw_tyrell_corp_shopify_products') }}
)
SELECT * FROM raw_source