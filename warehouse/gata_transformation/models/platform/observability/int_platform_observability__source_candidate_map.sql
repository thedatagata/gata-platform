{{ config(materialized='table') }}

{%- set tenants = [
    {'slug': 'tyrell_corp', 'status': 'active'},
    {'slug': 'wayne_enterprises', 'status': 'active'},
    {'slug': 'stark_industries', 'status': 'active'}
] -%}

WITH physical_inventory AS (
    SELECT * FROM {{ ref('int_platform_observability__md_table_stats') }}
),

discovered_assets AS (
    {% for client in tenants -%}
    SELECT
        '{{ client.slug }}' AS client_slug,
        '{{ client.status }}' AS tenant_status,
        pi.project_id,
        pi.source_schema,
        pi.source_table,
        pi.created_at
    FROM physical_inventory pi
    WHERE pi.source_schema = '{{ client.slug }}'
       OR pi.source_table LIKE '{{ client.slug }}_%'
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),

dbt_definitions AS (
    SELECT
        dbt_schema AS configured_schema,
        model_name AS table_name,
        node_id AS model_node_id,
        'true' AS is_integrated
    FROM {{ ref('int_platform_observability__model_definitions') }}
    WHERE node_id LIKE 'source.%'
),

dbt_health AS (
    SELECT
        source_name,
        source_table,
        freshness_status,
        RANK() OVER (
            PARTITION BY source_name, source_table
            ORDER BY generated_at DESC
        ) AS latest_check
    FROM {{ ref('int_platform_observability__source_freshness_results') }}
)

SELECT
    a.client_slug,
    a.tenant_status,
    a.source_schema,
    a.source_table,
    a.created_at,
    COALESCE(d.is_integrated, 'false') AS integrated_flag,
    h.freshness_status AS dbt_freshness_status,
    CASE
        WHEN d.is_integrated = 'true'
         AND h.freshness_status IN ('warn', 'error')
         THEN 'Integrated - Stale/Failing'
        WHEN COALESCE(d.is_integrated, 'false') = 'false'
         AND a.created_at IS NOT NULL
         THEN 'Strong Candidate - Active'
        ELSE 'Neutral'
    END AS candidate_priority_score
FROM discovered_assets a
LEFT JOIN dbt_definitions d
    ON a.source_schema = d.configured_schema
    AND a.source_table = d.table_name
LEFT JOIN dbt_health h
    ON a.source_schema = h.source_name
    AND a.source_table = h.source_table
    AND h.latest_check = 1
