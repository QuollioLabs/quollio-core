WITH columns AS (
    SELECT
      current_database() as database
      , schemaname
      , tablename
      , "column" columnname
      , type
      , has_table_privilege('{{ var("exec_user") }}', schemaname || '.' || tablename, 'select') is_selectable
    FROM
      pg_table_def 
    WHERE
      schemaname not in ('information_schema', 'pg_catalog')
    GROUP BY
      current_database()
      , schemaname
      , tablename
      , "column"
      , type
)
SELECT
  database as databasename
  , schemaname
  , tablename
  , columnname
  , case when type = 'boolean' then true else false end as is_bool
  , case when type in('smallint', 'int2', 'integer',
                      'int', 'int4', 'bigint', 'int8') THEN true
         when type like 'double%' 
           or type like 'numeric%'
           or type like 'decimal%' then true
         else false END AS is_calculable
FROM
  columns
WHERE
  is_selectable = true
  AND tablename not like 'custom_stl_%'
  AND tablename not like 'custom_sv%'
