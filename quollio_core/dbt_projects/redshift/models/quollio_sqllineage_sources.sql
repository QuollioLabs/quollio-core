with insert_table as (
    select
        sti.database as target_database
        , sti.schema as target_schema
        , sti.table as target_table
        , si.query
        , si.starttime
        , rank() over (partition by sti.database, sti.schema, sti.table order by si.starttime desc) rank
    from
        {{ source('pg_catalog', 'stl_insert') }} si
    right outer join
        {{ source('pg_catalog', 'svv_table_info') }} sti
    on
        sti.table_id = si.tbl
), latest_insert_table as (
    select
      *
    from
      insert_table
    where
      rank = 1
), scan as (
    select
        distinct
        query
    from
         {{ source('pg_catalog', 'stl_scan') }}
    where
        type = 2 -- scan object is user table
)
select
  lit.target_database as database_name
  , lit.target_schema as schema_name
  , lit.target_table as table_name
  , qt.text as query_text
from
  latest_insert_table lit
inner join
  {{ source('pg_catalog', 'stl_querytext') }} qt
on
  lit.query = qt.query
inner join
  scan sc
on
  lit.query = sc.query
