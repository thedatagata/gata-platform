WITH raw_source AS (
    SELECT *
    FROM {{ source('tyrell_corp_instagram_ads_raw', 'raw_tyrell_corp_instagram_ads_campaigns') }}
)
SELECT * FROM raw_source