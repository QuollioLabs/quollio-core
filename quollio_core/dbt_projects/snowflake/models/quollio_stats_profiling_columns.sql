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
), m_view_sys_columns AS (
  SELECT
    cols.table_catalog
    , cols.table_schema
    , cols.table_name
    , cols.column_name
    , cols.data_type
  FROM
    {{ source('account_usage', 'COLUMNS') }} cols
  LEFT OUTER JOIN
    {{ source('account_usage', 'TABLES') }} tbls
  ON
    cols.table_catalog = tbls.table_catalog
    AND cols.table_schema = tbls.table_schema
    AND cols.table_name = tbls.table_name
  WHERE
    tbls.table_type = 'MATERIALIZED VIEW'
    AND cols.column_name = 'SYS_MV_SOURCE_PARTITION'
), implicit_columns_removed AS (
  SELECT
    c.table_catalog
    , c.table_schema
    , c.table_name
    , c.column_name
    , c.data_type
  FROM
    columns c
  INNER JOIN
    accessible_tables a
  ON
    c.table_catalog = a.table_catalog
    AND c.table_schema = a.table_schema
    AND c.table_name = a.name
  MINUS
  SELECT
    table_catalog
    , table_schema
    , table_name
    , column_name
    , data_type
  FROM
    m_view_sys_columns
), final AS (
  SELECT
    table_catalog
    , table_schema
    , table_name
    , column_name
    , data_type
    , case when data_type in('NUMBER','DECIMAL', 'DEC', 'NUMERIC',
                             'INT', 'INTEGER', 'BIGINT', 'SMALLINT',
                             'TINYINT', 'BYTEINT')
                             THEN true
           else false END AS is_calculable
  FROM
    implicit_columns_removed
)
select * from final
