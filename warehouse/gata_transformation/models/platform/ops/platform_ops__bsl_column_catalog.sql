{{ config(materialized='table') }}

-- Flattens the JSON semantic_manifest from platform_ops__boring_semantic_layer
-- into one row per column with deterministic semantic classification.
-- Mirrors bsl_model_builder._classify_column() logic in warehouse-native SQL.

WITH base AS (
    SELECT
        bsl.tenant_slug,
        bsl.table_type,
        bsl.subject,
        bsl.table_name,
        unnested.column_name,
        unnested.data_type,
        unnested.ordinal_position
    FROM {{ ref('platform_ops__boring_semantic_layer') }} bsl,
    LATERAL (
        SELECT unnest(
            from_json(
                bsl.semantic_manifest,
                '[{"column_name":"VARCHAR","data_type":"VARCHAR","ordinal_position":"INTEGER"}]'
            )
        ) AS unnested
    )
    WHERE bsl.tenant_slug IS NOT NULL
),

classified AS (
    SELECT
        tenant_slug,
        table_type,
        subject,
        table_name,
        column_name,
        data_type,
        ordinal_position,

        -- semantic_role: dimension or measure
        CASE
            -- Skip columns (filtered in WHERE below)
            WHEN column_name IN ('tenant_slug') THEN 'skip'
            WHEN data_type IN ('JSON', 'BLOB') THEN 'skip'

            -- Clear dimension types
            WHEN data_type IN ('VARCHAR', 'TEXT', 'DATE', 'TIMESTAMP', 'BOOLEAN', 'BOOL')
                THEN 'dimension'

            -- Clear measure types
            WHEN data_type IN ('DOUBLE', 'FLOAT', 'DECIMAL', 'REAL')
                THEN 'measure'

            -- Integer types: disambiguate by column name patterns
            WHEN data_type IN ('BIGINT', 'INTEGER', 'INT', 'SMALLINT', 'TINYINT', 'HUGEINT') THEN
                CASE
                    -- Dimension patterns (IDs, keys, names, statuses)
                    WHEN column_name LIKE '%\_id' ESCAPE '\'
                      OR column_name LIKE '%\_key' ESCAPE '\'
                      OR column_name LIKE '%\_slug' ESCAPE '\'
                      OR column_name LIKE '%\_name' ESCAPE '\'
                      OR column_name LIKE '%\_status' ESCAPE '\'
                      OR column_name LIKE '%\_type' ESCAPE '\'
                      OR column_name LIKE '%\_category' ESCAPE '\'
                      OR column_name LIKE '%\_email' ESCAPE '\'
                      OR column_name LIKE '%\_source' ESCAPE '\'
                      OR column_name LIKE '%\_medium' ESCAPE '\'
                      OR column_name LIKE '%\_campaign' ESCAPE '\'
                      OR column_name LIKE '%\_country' ESCAPE '\'
                      OR column_name LIKE '%\_device' ESCAPE '\'
                        THEN 'dimension'
                    -- Measure patterns (counts, totals, amounts)
                    WHEN column_name LIKE 'total\_%' ESCAPE '\'
                      OR column_name LIKE 'count\_%' ESCAPE '\'
                      OR column_name LIKE 'num\_%' ESCAPE '\'
                      OR column_name LIKE 'sum\_%' ESCAPE '\'
                      OR column_name LIKE 'events\_in\_%' ESCAPE '\'
                      OR column_name LIKE '%revenue%'
                      OR column_name LIKE '%spend%'
                      OR column_name LIKE '%impressions%'
                      OR column_name LIKE '%clicks%'
                      OR column_name LIKE '%conversions%'
                      OR column_name LIKE '%price%'
                      OR column_name LIKE '%amount%'
                      OR column_name LIKE '%cost%'
                      OR column_name LIKE '%duration%'
                        THEN 'measure'
                    -- Default: integers with no clear pattern → measure
                    ELSE 'measure'
                END

            -- Unknown type → dimension (safe default)
            ELSE 'dimension'
        END AS semantic_role,

        -- bsl_type for frontend adapter (string|date|timestamp|boolean|number)
        CASE
            WHEN data_type IN ('VARCHAR', 'TEXT') THEN 'string'
            WHEN data_type = 'DATE' THEN 'date'
            WHEN data_type = 'TIMESTAMP' THEN 'timestamp'
            WHEN data_type IN ('BOOLEAN', 'BOOL') THEN 'boolean'
            ELSE 'number'
        END AS bsl_type,

        -- is_time_dimension
        CASE
            WHEN data_type IN ('DATE', 'TIMESTAMP') THEN TRUE
            ELSE FALSE
        END AS is_time_dimension

    FROM base
)

SELECT
    c.tenant_slug,
    c.table_type,
    c.subject,
    c.table_name,
    c.column_name,
    c.data_type,
    c.ordinal_position,
    c.semantic_role,
    c.bsl_type,
    c.is_time_dimension,

    -- inferred_agg (measures only): sum | avg | count_distinct
    CASE
        WHEN c.semantic_role != 'measure' THEN NULL
        WHEN c.column_name LIKE '%duration%'
          OR c.column_name LIKE '%avg%'
            THEN 'avg'
        WHEN c.column_name LIKE '%\_id' ESCAPE '\'
            THEN 'count_distinct'
        ELSE 'sum'
    END AS inferred_agg

FROM classified c
WHERE c.semantic_role != 'skip'
ORDER BY c.tenant_slug, c.table_name, c.ordinal_position
