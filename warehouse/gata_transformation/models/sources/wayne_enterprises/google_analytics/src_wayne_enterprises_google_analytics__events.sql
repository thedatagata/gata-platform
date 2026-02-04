WITH raw_source AS (
    SELECT *
    FROM {{ source('wayne_enterprises_google_analytics_raw', 'raw_wayne_enterprises_google_analytics_events') }}
)
SELECT * FROM raw_source