WITH raw_source AS (
    SELECT *
    FROM {{ source('tyrell_corp_facebook_ads_raw', 'raw_tyrell_corp_facebook_ads_ads') }}
)
SELECT * FROM raw_source