WITH raw_source AS (
    SELECT *
    FROM {{ source('wayne_enterprises_shopify_raw', 'raw_wayne_enterprises_shopify_products') }}
)
SELECT * FROM raw_source