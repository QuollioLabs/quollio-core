-- Gets  full table lineage from Databricks
WITH table_lineage_history AS (
  SELECT
    source_table_full_name as upstream_table,
    target_table_full_name as downstream_table,
    target_type,
    event_time,
    RANK() OVER (
      PARTITION BY target_table_full_name
      ORDER BY
        event_time DESC
    ) AS rank
  FROM
    {{ source('access','table_lineage')  }}
  WHERE
    source_table_full_name IS NOT NULL
    AND target_table_full_name IS NOT NULL
    AND source_table_full_name NOT LIKE "%quollio%"
    AND target_table_full_name NOT LIKE "%quollio%"
),
-- Gets list of existing tables in catalogs
existing_tables (
  SELECT
    CONCAT(table_catalog, '.', table_schema, '.', table_name) AS table_full_name
  FROM
   {{ source('inf_sch','tables')  }}
),

-- Checks if the downstream tables exists and group operations.
downstream_table_exists (
  SELECT
    upstream_table,
    downstream_table,
    target_type,
    event_time
  FROM
    table_lineage_history tlh
    INNER JOIN existing_tables et ON tlh.downstream_table = et.table_full_name
  WHERE
    rank = 1
  GROUP BY upstream_table, downstream_table, target_type, event_time
),

aggregated_table_lineage AS (
  SELECT
      downstream_table,
      collect_set(
          named_struct(
              'upstream_object_name', upstream_table
          )
      ) AS upstream_tables
  FROM
    downstream_table_exists
  GROUP BY
      downstream_table
)
SELECT
  downstream_table as DOWNSTREAM_TABLE_NAME,
  to_json(upstream_tables) as UPSTREAM_TABLES

FROM
  aggregated_table_lineage
  