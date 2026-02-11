{{ config(materialized='table') }}

WITH combined_users AS (
    SELECT
        'tyrell_corp' AS tenant_slug,
        user_pseudo_id,
        customer_email,
        is_customer,
        total_events,
        total_sessions,
        CAST('{{ var("dlt_load_id", "manual_run") }}' AS VARCHAR) AS dlt_load_id
    FROM {{ ref('dim_tyrell_corp__users') }}

    UNION ALL

    SELECT
        'wayne_enterprises' AS tenant_slug,
        user_pseudo_id,
        customer_email,
        is_customer,
        total_events,
        total_sessions,
        CAST('{{ var("dlt_load_id", "manual_run") }}' AS VARCHAR) AS dlt_load_id
    FROM {{ ref('dim_wayne_enterprises__users') }}

    UNION ALL

    SELECT
        'stark_industries' AS tenant_slug,
        user_pseudo_id,
        customer_email,
        is_customer,
        total_events,
        total_sessions,
        CAST('{{ var("dlt_load_id", "manual_run") }}' AS VARCHAR) AS dlt_load_id
    FROM {{ ref('dim_stark_industries__users') }}
)

SELECT
    dlt_load_id,
    tenant_slug,
    COUNT(*) AS total_users,
    COUNT(CASE WHEN is_customer THEN 1 END) AS resolved_customers,
    COUNT(CASE WHEN NOT is_customer THEN 1 END) AS anonymous_users,
    ROUND(
        COUNT(CASE WHEN is_customer THEN 1 END)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100,
    2) AS identity_resolution_rate,
    SUM(total_events) AS total_events,
    SUM(total_sessions) AS total_sessions
FROM combined_users
GROUP BY 1, 2