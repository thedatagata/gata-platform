WITH raw_source AS (
    SELECT *
    FROM {{ source('tyrell_corp_google_analytics_raw', 'raw_tyrell_corp_google_analytics_events') }}
)
SELECT * FROM raw_source