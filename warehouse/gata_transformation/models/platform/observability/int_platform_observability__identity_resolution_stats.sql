{{ config(materialized='table') }}

WITH combined_sessions AS (
    SELECT
        'tyrell_corp' AS tenant_slug,
        user_pseudo_id,
        user_id,
        CAST('{{ var("dlt_load_id", "manual_run") }}' AS VARCHAR) AS dlt_load_id
    FROM {{ ref('fct_tyrell_corp__sessions') }}

    UNION ALL

    SELECT
        'wayne_enterprises' AS tenant_slug,
        user_pseudo_id,
        user_id,
        CAST('{{ var("dlt_load_id", "manual_run") }}' AS VARCHAR) AS dlt_load_id
    FROM {{ ref('fct_wayne_enterprises__sessions') }}

    UNION ALL

    SELECT
        'stark_industries' AS tenant_slug,
        user_pseudo_id,
        user_id,
        CAST('{{ var("dlt_load_id", "manual_run") }}' AS VARCHAR) AS dlt_load_id
    FROM {{ ref('fct_stark_industries__sessions') }}
)

SELECT
    dlt_load_id,
    tenant_slug,
    COUNT(*) AS total_sessions,
    COUNT(CASE WHEN user_id IS NOT NULL AND user_id != '' THEN 1 END) AS known_user_sessions,
    COUNT(CASE WHEN user_id IS NULL OR user_id = '' THEN 1 END) AS anonymous_sessions,
    ROUND(
        COUNT(CASE WHEN user_id IS NOT NULL AND user_id != '' THEN 1 END)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100,
    2) AS stitched_user_percentage
FROM combined_sessions
GROUP BY 1, 2
