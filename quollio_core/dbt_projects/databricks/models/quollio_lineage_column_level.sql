-- Gets  full table lineage from Databricks
WITH columns_lineage_history AS (
  SELECT
    -- The databricks columns table does not have a full table name, create with CONCAT()
    source_table_full_name AS upstream_table,
    target_table_full_name as downstream_table,
    source_column_name as upstream_column,
    target_column_name as downstream_column,
    event_time,
    RANK() OVER (
      PARTITION BY target_table_full_name
      ORDER BY
        event_time DESC
    ) AS rank
  FROM
    {{ source('access','column_lineage')  }}
  WHERE
    source_table_full_name IS NOT NULL
    AND target_table_full_name IS NOT NULL
    AND source_table_full_name NOT LIKE "%quollio%"
    AND target_table_full_name NOT LIKE "%quollio%"
),
-- Gets list of existing columns in catalogs
existing_columns (
  SELECT
    CONCAT(table_catalog, '.', table_schema, '.', table_name) AS table_full_name,
    column_name
  FROM
   {{ source('inf_sch','columns')  }}
),

-- Checks if the downstream tables exists and group operations.
downstream_column_exists (
  SELECT
    upstream_table AS UPSTREAM_TABLE_NAME,
    upstream_column AS UPSTREAM_COLUMN_NAME,
    downstream_table AS DOWNSTREAM_TABLE_NAME,
    downstream_column AS DOWNSTREAM_COLUMN_NAME,
    event_time
  FROM
    columns_lineage_history clh
    INNER JOIN existing_columns ec ON clh.downstream_table = ec.table_full_name
    AND clh.downstream_column = ec.column_name
  WHERE
    rank = 1
  GROUP BY UPSTREAM_TABLE, UPSTREAM_COLUMN, DOWNSTREAM_TABLE, DOWNSTREAM_COLUMN, EVENT_TIME
),

-- Aggregates the column lineage
aggregated_column_lineage AS (
  SELECT
      downstream_table_name,
      downstream_column_name,
      collect_set(
          named_struct(
              'upstream_table_name', upstream_table_name,
              'upstream_column_name', upstream_column_name
          )
      ) AS upstream_columns
  FROM
    downstream_column_exists
  GROUP BY
      downstream_table_name,
      downstream_column_name
)

SELECT
  downstream_table_name AS DOWNSTREAM_TABLE_NAME,
  downstream_column_name AS DOWNSTREAM_COLUMN_NAME,
  to_json(upstream_columns) AS UPSTREAM_COLUMNS
FROM
  aggregated_column_lineage
  