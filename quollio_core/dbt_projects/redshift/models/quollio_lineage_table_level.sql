with target_tables as (
    select
        sti.database as target_database
        , sti.schema as target_schema
        , sti.table as target_table
        , si.query
        , si.starttime
        , rank() over (partition by sti.database, sti.schema, sti.table order by si.starttime desc) rank
    from
        {{ source('pg_catalog', 'stl_insert') }} si
    inner join
        {{ source('pg_catalog', 'svv_table_info') }} sti
    on
        sti.table_id = si.tbl
    where
        target_table not like 'quollio_%'
), latest_target_tables as (
    select
        target_database
        , target_schema
        , target_table
        , query
    from
        target_tables
    where
        rank = 1
), scan as (
    select
        distinct
        userid
        , query
        , tbl
        , type as scan_type
    from
        {{ source('pg_catalog', 'stl_scan') }}
    where
        type in (1, 2, 3)
), source_tables as (
select
    sti.database as source_database
    , sti.schema as source_schema
    , sti.table as source_table
    , sq.query as query
    , sq.starttime as st
    , rank() over (partition by sti.database, sti.schema, sti.table order by sq.starttime desc) rank
from
    scan sc
join
    {{ source('pg_catalog', 'svv_table_info') }} sti
on
    sti.table_id = sc.tbl
left join
    stl_query sq
on
    sc.query = sq.query
left join
    svl_user_info sui
on
    sq.userid = sui.usesysid
where
    sui.usename <> 'rdsdb'
), latest_source_tables as (
select
    source_database
    , source_schema
    , source_table
    , query
from
    source_tables
where
    rank = 1
), table_lineage as (
select
    distinct
    target_database
    , target_schema
    , target_table
    , source_database
    , source_schema
    , source_table
from
    target_tables
inner join
    source_tables
using
    (query)
)
select
    target_database || '.' || target_schema || '.' || target_table as downstream_table_name
    , source_database || '.' || source_schema || '.' || source_table as upstream_table_name
from
    table_lineage
