WITH columns AS (
    SELECT
      current_database() as database_name
      , "schemaname" as schema_name
      , "tablename" as table_name
      , "column" as column_name
      , "type" as data_type
      , has_table_privilege('{{ var("query_user") }}', "schemaname" || '.' || "tablename", 'select') is_selectable
    FROM
      {{ source('pg_catalog', 'pg_table_def') }}
    WHERE
      "schemaname" not in ('information_schema')
      and "schemaname" not like 'pg_%%'
      and "tablename" not like 'quollio_%%'
    GROUP BY
      current_database()
      , "schemaname"
      , "tablename"
      , "column"
      , "type"
), tables AS (
    SELECT
      current_database() as database_name
      , "schemaname" as schema_name
      , "tablename" as table_name
    FROM
      {{ source('pg_catalog', 'pg_tables') }}
    WHERE
      "schemaname" not in ('information_schema')
      and "schemaname" not like 'pg_%%'
      and "tablename" not like 'quollio_%%'
)
SELECT
  columns.database_name
  , columns.schema_name
  , columns.table_name
  , columns.column_name
  , case when columns.data_type = 'boolean' then true else false end as is_bool
  , case when columns.data_type in('smallint', 'int2', 'integer',
                      'int', 'int4', 'bigint', 'int8') THEN true
         when columns.data_type like 'double%'
           or columns.data_type like 'numeric%'
           or columns.data_type like 'decimal%' then true
         else false END AS is_calculable
FROM
  tables
INNER JOIN
  columns USING (database_name, schema_name, table_name)
WHERE
  columns.is_selectable = true
