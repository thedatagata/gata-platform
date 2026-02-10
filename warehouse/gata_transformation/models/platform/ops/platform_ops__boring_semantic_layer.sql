{{ config(materialized='table') }}

-- Catalogs star schema (fct_* and dim_*) tables with column metadata
WITH star_schema_tables AS (
    SELECT
        table_schema,
        table_name,
        CASE
            WHEN table_name LIKE 'fct_%' THEN
                regexp_extract(table_name, 'fct_(.+?)__', 1)
            WHEN table_name LIKE 'dim_%' THEN
                regexp_extract(table_name, 'dim_(.+?)__', 1)
        END AS tenant_slug,
        CASE
            WHEN table_name LIKE 'fct_%' THEN 'fact'
            WHEN table_name LIKE 'dim_%' THEN 'dimension'
        END AS table_type,
        CASE
            WHEN table_name LIKE 'fct_%' THEN
                regexp_extract(table_name, '__(.+)$', 1)
            WHEN table_name LIKE 'dim_%' THEN
                regexp_extract(table_name, '__(.+)$', 1)
        END AS subject
    FROM information_schema.tables
    WHERE table_name LIKE 'fct_%' OR table_name LIKE 'dim_%'
),

column_metadata AS (
    SELECT
        c.table_schema,
        c.table_name,
        list(
            {
                'column_name': c.column_name,
                'data_type': c.data_type,
                'ordinal_position': c.ordinal_position
            }
            ORDER BY c.ordinal_position
        ) AS columns_json
    FROM information_schema.columns c
    INNER JOIN star_schema_tables s
        ON c.table_schema = s.table_schema
        AND c.table_name = s.table_name
    GROUP BY c.table_schema, c.table_name
)

SELECT
    s.tenant_slug,
    s.table_type,
    s.subject,
    s.table_name,
    CAST(cm.columns_json AS JSON) AS semantic_manifest
FROM star_schema_tables s
LEFT JOIN column_metadata cm
    ON s.table_schema = cm.table_schema
    AND s.table_name = cm.table_name
WHERE s.tenant_slug IS NOT NULL
