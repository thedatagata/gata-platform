{%- macro get_session_attribution_logic(source_model, conversion_model) -%}
WITH utm_values AS (
  SELECT
    session_id,
    page_viewed_at,
    user_id,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    utm_term
  FROM
    {{ source_model }}
  {%- if this is not none and is_incremental() and execute -%}
  WHERE page_viewed_at >= (SELECT MAX(session_started_at) FROM {{ this }})
  {%- endif -%}
),
stitch AS (
  SELECT
    session_id,
    user_id,
    LAST_VALUE(utm_source IGNORE NULLS) OVER(PARTITION BY session_id ORDER BY page_viewed_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) utm_source,
    LAST_VALUE(utm_medium IGNORE NULLS) OVER(PARTITION BY session_id ORDER BY page_viewed_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) utm_medium,
    LAST_VALUE(utm_campaign IGNORE NULLS) OVER(PARTITION BY session_id ORDER BY page_viewed_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) utm_campaign,
    LAST_VALUE(utm_content IGNORE NULLS) OVER(PARTITION BY session_id ORDER BY page_viewed_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) utm_content,
    LAST_VALUE(utm_term IGNORE NULLS) OVER(PARTITION BY session_id ORDER BY page_viewed_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) utm_term,
    FIRST_VALUE(page_viewed_at) OVER(PARTITION BY session_id ORDER BY page_viewed_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) session_started_at,
    LAST_VALUE(page_viewed_at) OVER(PARTITION BY session_id ORDER BY page_viewed_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) session_ended_at,
    COUNT(DISTINCT session_id) OVER(PARTITION BY user_id) AS sessions_count,
    1 AS page_views
  FROM
    utm_values
),
sessions_attrib AS (
  SELECT
    session_id,
    user_id,
    COALESCE(utm_source, '(not set)') utm_source,
    COALESCE(utm_medium, '(not set)') utm_medium,
    COALESCE(utm_campaign, '(not set)') utm_campaign,
    COALESCE(utm_content, '(not set)') utm_content,
    COALESCE(utm_term, '(not set)') utm_term,
    session_started_at,
    session_ended_at,
    SUM(page_views) pageviews
  FROM
    stitch
  GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9
),
conversions AS (
  SELECT
    application_id,
    user_id,
    application_created_at AS conversion_time,
    funding_amount
  FROM
    {{ conversion_model }}
  WHERE
    application_created_at IS NOT NULL
),
session_conversion_pairs AS (
  SELECT
    sa.session_id,
    sa.user_id,
    sa.utm_source,
    sa.utm_medium,
    sa.utm_campaign,
    sa.utm_content,
    sa.utm_term,
    sa.session_started_at,
    sa.session_ended_at,
    sa.pageviews,
    c.application_id,
    c.conversion_time,
    c.funding_amount,
    TIMESTAMP_DIFF(c.conversion_time, sa.session_started_at, DAY) AS days_to_conversion,
    CASE
      WHEN c.conversion_time BETWEEN sa.session_started_at AND sa.session_ended_at THEN TRUE
      ELSE FALSE
    END AS application_session,
    CASE
      WHEN c.conversion_time > sa.session_started_at THEN TRUE
      ELSE FALSE
    END AS prospect_session,
    CASE
      WHEN c.conversion_time BETWEEN sa.session_started_at AND sa.session_ended_at AND c.funding_amount > 0 THEN TRUE
      ELSE FALSE
    END AS application_funded_session
  FROM
    sessions_attrib sa
  INNER JOIN
    conversions c
  ON
    sa.user_id = c.user_id
    AND sa.session_started_at BETWEEN (c.conversion_time - INTERVAL 30 DAY) AND c.conversion_time
),
data AS (
  SELECT
    scp.*,
    CASE
      WHEN session_id = FIRST_VALUE(session_id) OVER (PARTITION BY user_id ORDER BY session_started_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) THEN 1
      ELSE 0
    END AS first_click_attrib_pct,
    CASE
      WHEN session_id = LAST_VALUE(session_id) OVER (PARTITION BY application_id ORDER BY session_started_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) THEN 1
      ELSE 0
    END AS last_click_attrib_pct,
    1.0 / COUNT(session_id) OVER (PARTITION BY application_id) AS even_click_attrib_pct,
    CASE
      WHEN session_started_at > (conversion_time - INTERVAL 7 day) THEN 1.0
      WHEN session_started_at > (conversion_time - INTERVAL 14 day) THEN 0.5
      WHEN session_started_at > (conversion_time - INTERVAL 21 day) THEN 0.25
      WHEN session_started_at > (conversion_time - INTERVAL 28 day) THEN 0.125
      ELSE 0.0625
    END AS time_decay_attrib_pct
  FROM
    session_conversion_pairs scp
),
AggregatedPaths AS (
  SELECT
      application_id,
      ARRAY_TO_STRING(ARRAY_AGG(utm_source ORDER BY session_started_at ASC), ' > ') AS utm_source_path,
      ARRAY_TO_STRING(ARRAY_AGG(utm_medium ORDER BY session_started_at ASC), ' > ') AS utm_medium_path
  FROM
      data
  GROUP BY
      application_id
)
SELECT
   d.*,
   ap.utm_source_path,
   ap.utm_medium_path
FROM
   data d
LEFT JOIN
   AggregatedPaths ap ON d.application_id = ap.application_id
{%- endmacro -%}