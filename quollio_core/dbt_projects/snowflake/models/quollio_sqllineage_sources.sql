with downstream_tables as (
    select
        query_id
        , om.value:"objectName"::varchar as downstream_table_name
        , split(om.value:"objectName"::varchar, '.') as downstream_table_name_list
        , rank() over (partition by downstream_table_name order by query_start_time desc) as query_exec_time_rank
    from
        {{ source('account_usage', 'ACCESS_HISTORY') }} ah
        , lateral flatten (input => ah.OBJECTS_MODIFIED) om
    where
        om.value:"objectId" is not null
        AND om.value:"objectName" NOT LIKE '%.GE_TMP_%'
        AND om.value:"objectName" NOT LIKE '%.GE_TEMP_%'
        AND om.value:"objectName" NOT LIKE '%QUOLLIO_%'
), latest_downstream_tables as (
    select
        distinct
        downstream_table_name_list
    	, query_id
    from
        downstream_tables
    where
        query_exec_time_rank = 1
), query_text as (
    SELECT 
        query_id
        , query_text
        , query_type
        , database_name
        , schema_name
    FROM
        {{ source('account_usage', 'QUERY_HISTORY') }}
    where
        execution_status = 'SUCCESS'
    and
        query_type in ('CREATE_TABLE_AS_SELECT', 'MERGE')
)
select
    lst.downstream_table_name_list[0]::varchar database_name
    , lst.downstream_table_name_list[1]::varchar schema_name
    , lst.downstream_table_name_list[2]::varchar table_name
    , qt.query_text
from
    latest_downstream_tables lst
left outer join
    query_text qt
on
    lst.query_id = qt.query_id
where
    qt.query_id is not null
