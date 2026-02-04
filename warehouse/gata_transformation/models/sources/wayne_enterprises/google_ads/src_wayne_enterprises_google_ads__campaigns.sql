WITH raw_source AS (
    SELECT *
    FROM {{ source('wayne_enterprises_google_ads_raw', 'raw_wayne_enterprises_google_ads_campaigns') }}
)
SELECT * FROM raw_source