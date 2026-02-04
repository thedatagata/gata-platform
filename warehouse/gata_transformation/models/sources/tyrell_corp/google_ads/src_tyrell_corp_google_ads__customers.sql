WITH raw_source AS (
    SELECT *
    FROM {{ source('tyrell_corp_google_ads_raw', 'raw_tyrell_corp_google_ads_customers') }}
)
SELECT * FROM raw_source