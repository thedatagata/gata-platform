{{ config(materialized='table') }}

{%- set all_clients = get_all_clients() -%}

WITH physical_inventory AS (
    SELECT * FROM {{ ref('int_platform_observability__md_table_stats') }}
),

-- Discovery: Targeted lookup based on manifest slugs
discovered_assets AS (
    {% for client in all_clients -%}
    SELECT 
        '{{ client.slug }}' as client_slug,
        '{{ client.status }}' as tenant_status,
        *
    FROM physical_inventory
    WHERE source_schema = '{{ client.slug }}' 
       OR source_table LIKE '{{ client.slug }}_%'
    {{ "UNION ALL " if not loop.last }}
    {%- endfor %}
),

dbt_definitions AS (
    SELECT 
        dbt_schema as configured_schema,
        model_name as table_name,
        node_id as model_node_id,
        'true' as is_integrated
    FROM {{ ref('int_platform_observability__model_definitions') }}
    WHERE node_id LIKE 'source.%'
),

dbt_health AS (
    SELECT 
        source_name,
        source_table,
        freshness_status,
        RANK() OVER (PARTITION BY source_name, source_table ORDER BY generated_at DESC) as latest_check
    FROM {{ ref('int_platform_observability__source_freshness_results') }}
)

SELECT
    a.client_slug,
    a.tenant_status,
    a.source_schema,
    a.source_table,
    a.created_at,
    COALESCE(d.is_integrated, 'false') as integrated_flag,
    h.freshness_status as dbt_freshness_status,
    
    -- Evaluation Logic
    CASE 
        -- Scenario A: Already Integrated but failing health checks
        WHEN d.is_integrated = 'true' 
         AND h.freshness_status IN ('warn', 'error')
         THEN 'Integrated - Stale/Failing'
        
        -- Scenario B: High Probability New Source (Recent Activity/Creation)
        WHEN COALESCE(d.is_integrated, 'false') = 'false' 
         AND to_timestamp(a.created_at) > (current_timestamp - INTERVAL 72 HOUR)
         THEN 'Strong Candidate - Active'

        -- Scenario C: Potential New Client "Shell" (Recent Creation)
        WHEN COALESCE(d.is_integrated, 'false') = 'false' 
         AND to_timestamp(a.created_at) > (current_timestamp - INTERVAL 48 HOUR)
         THEN 'Strong Candidate - New Shell'
         
        -- Scenario D: Legacy/Zombie Data
        WHEN to_timestamp(a.created_at) < (current_timestamp - INTERVAL 30 DAY)
         THEN 'Likely Stale'
         
        ELSE 'Neutral'
    END as candidate_priority_score
FROM discovered_assets a
LEFT JOIN dbt_definitions d
    ON a.source_schema = d.configured_schema
    AND a.source_table = d.table_name
LEFT JOIN dbt_health h
    ON a.source_schema = h.source_name
    AND a.source_table = h.source_table
    AND h.latest_check = 1