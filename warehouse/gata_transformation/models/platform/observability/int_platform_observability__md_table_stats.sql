{{ config(materialized='table') }}

SELECT
    table_catalog as project_id,
    table_schema as source_schema,
    table_name as source_table,
    NULL as created_at  -- DuckDB information_schema doesn't have creation_time
FROM information_schema.tables
WHERE table_schema NOT LIKE '%dbt%'
  AND table_schema NOT IN ('information_schema', 'pg_catalog', 'main')