WITH raw_source AS (
    SELECT *
    FROM {{ source('tyrell_corp_stripe_raw', 'raw_tyrell_corp_stripe_charges') }}
)
SELECT * FROM raw_source