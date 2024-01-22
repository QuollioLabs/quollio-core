with view_relations as (
select
    distinct
    current_database() as db
    , tgt_nsp.nspname as target_schema
    , tgt_obj.relname as target_table
    , src_nsp.nspname as source_schema
    , src_obj.relname as source_table
from
    {{ source('pg_catalog', 'pg_class') }} as src_obj
inner join
    {{ source('pg_catalog', 'pg_depend') }} as src_dep
on
    src_obj.oid = src_dep.refobjid
inner join
    {{ source('pg_catalog', 'pg_depend') }} as tgt_dep
on
    src_dep.objid = tgt_dep.objid
join
    {{ source('pg_catalog', 'pg_class') }} as tgt_obj
on
    tgt_dep.refobjid = tgt_obj.oid
    and src_obj.oid <> tgt_obj.oid
left outer join
    {{ source('pg_catalog', 'pg_namespace') }} as src_nsp
on
    src_obj.relnamespace = src_nsp.oid
left outer join
    {{ source('pg_catalog', 'pg_namespace') }} tgt_nsp
on
    tgt_obj.relnamespace = tgt_nsp.oid
where
    tgt_dep.deptype = 'i'
    and tgt_obj.relkind = 'v'
    and tgt_nsp.nspname not in ('pg_catalog', 'information_schema')
    and src_nsp.nspname not in ('pg_catalog', 'information_schema')
    and tgt_obj.relname not like 'quollio_%'
)
select
    db || '.' || target_schema || '.' || target_table as downstream_table_name
    , db || '.' || source_schema || '.' || source_table as upstream_table_name
from
    view_relations
