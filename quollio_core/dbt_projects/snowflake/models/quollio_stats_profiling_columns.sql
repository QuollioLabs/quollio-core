WITH columns AS (
    SELECT
      table_catalog
      , table_schema
      , table_name
      , column_name
      , data_type
    FROM
      {{ source('account_usage', 'COLUMNS') }}
    WHERE
      deleted is null
      AND table_name NOT LIKE 'QUOLLIO_%%'
    GROUP BY
      table_catalog
      , table_schema
      , table_name
      , column_name
      , data_type
    ORDER BY
      table_catalog
      , table_schema
      , table_name
), accessible_tables AS (
    SELECT
      table_catalog
      , table_schema
      , name
    FROM
      {{ source('account_usage', 'GRANTS_TO_ROLES') }}
    WHERE
      granted_on in ('TABLE', 'MATERIALIZED VIEW')
      AND grantee_name = '{{ var("query_role") }}'
      AND privilege in ('SELECT', 'OWNERSHIP', 'REFERENCES')
      AND deleted_on IS NULL
    GROUP BY
      table_catalog
      , table_schema
      , name  
)

SELECT
  c.table_catalog
  , c.table_schema
  , c.table_name
  , c.column_name
  , c.data_type
  , case when c.data_type in('NUMBER','DECIMAL', 'DEC', 'NUMERIC',
                           'INT', 'INTEGER', 'BIGINT', 'SMALLINT',
                           'TINYINT', 'BYTEINT')
                           THEN true
         else false END AS is_calculable
FROM
  columns c
INNER JOIN
  accessible_tables a
ON
  c.table_catalog = a.table_catalog
  AND c.table_schema = a.table_schema
  AND c.table_name = a.name
