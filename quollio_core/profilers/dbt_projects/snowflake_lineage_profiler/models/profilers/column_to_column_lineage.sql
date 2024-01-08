WITH column_lineage_history as (
    SELECT
      directSources.value: "objectName"::varchar as upstream_object_name
      , directSources.value: "columnName"::varchar as upstream_column_name
      , om.value: "objectName"::varchar as downstream_table_name
      , columns_modified.value: "columnName"::varchar as downstream_column_name
      , rank() over (partition by downstream_table_name order by query_start_time desc) as query_exec_time_rank
    FROM
      snowflake.account_usage.access_history ah
      , lateral flatten(input => ah.OBJECTS_MODIFIED) om
      , lateral flatten(input => om.value: "columns", outer => true) columns_modified
      , lateral flatten(input => columns_modified.value: "directSources", outer => true) directSources
	WHERE
	    upstream_object_name IS NOT NULL
		AND directSources.value:"objectId" IS NOT NULL
		AND om.value:"objectId" IS NOT NULL
		AND om.value:"objectName" NOT LIKE '%.GE_TMP_%'
		AND om.value:"objectName" NOT LIKE '%.GE_TEMP_%'
		-- AND ah.query_start_time >= to_timestamp_ltz({start_time_millis}, 3)
		-- AND ah.query_start_time < to_timestamp_ltz({end_time_millis}, 3)
		AND (
			NOT RLIKE (
				upstream_object_name,
				'.*\.FIVETRAN_.*_STAGING\..*',
				'i'
			)
			AND upstream_object_name != downstream_table_name
			AND NOT RLIKE (upstream_object_name, '.*__DBT_TMP$', 'i')
			AND NOT RLIKE (upstream_object_name, '.*\.SEGMENT_.*', 'i')
			AND NOT RLIKE (upstream_object_name, '.*\.STAGING_.*_.*', 'i')
		)
		AND (
			NOT RLIKE (
				downstream_table_name,
				'.*\.FIVETRAN_.*_STAGING\..*',
				'i'
			)
			AND upstream_object_name != downstream_table_name
			AND NOT RLIKE (downstream_table_name, '.*__DBT_TMP$', 'i')
			AND NOT RLIKE (downstream_table_name, '.*\.SEGMENT_.*', 'i')
			AND NOT RLIKE (downstream_table_name, '.*\.STAGING_.*_.*', 'i')
		)
UNION
    SELECT
      baseSources.value: "objectName"::varchar as upstream_object_name
      , baseSources.value: "columnName"::varchar as upstream_column_name
      , om.value: "objectName"::varchar as downstream_table_name
      , columns_modified.value: "columnName"::varchar as downstream_column_name
      , rank() over (partition by downstream_table_name order by query_start_time desc) as query_exec_time_rank
    FROM
      snowflake.account_usage.access_history ah
      , lateral flatten(input => ah.OBJECTS_MODIFIED) om
      , lateral flatten(input => om.value: "columns", outer => true) columns_modified
      , lateral flatten(input => columns_modified.value: "baseSources", outer => true) baseSources
	WHERE
	    upstream_object_name is not null
		AND baseSources.value:"objectId" IS NOT NULL
		AND om.value:"objectId" IS NOT NULL
		AND om.value:"objectName" NOT LIKE '%.GE_TMP_%'
		AND om.value:"objectName" NOT LIKE '%.GE_TEMP_%'
		-- AND ah.query_start_time >= to_timestamp_ltz({start_time_millis}, 3)
		-- AND ah.query_start_time < to_timestamp_ltz({end_time_millis}, 3)
		AND (
			NOT RLIKE (
				upstream_object_name,
				'.*\.FIVETRAN_.*_STAGING\..*',
				'i'
			)
			AND upstream_object_name != downstream_table_name
			AND NOT RLIKE (upstream_object_name, '.*__DBT_TMP$', 'i')
			AND NOT RLIKE (upstream_object_name, '.*\.SEGMENT_.*', 'i')
			AND NOT RLIKE (upstream_object_name, '.*\.STAGING_.*_.*', 'i')
		)
		AND (
			NOT RLIKE (
				downstream_table_name,
				'.*\.FIVETRAN_.*_STAGING\..*',
				'i'
			)
			AND upstream_object_name != downstream_table_name
			AND NOT RLIKE (downstream_table_name, '.*__DBT_TMP$', 'i')
			AND NOT RLIKE (downstream_table_name, '.*\.SEGMENT_.*', 'i')
			AND NOT RLIKE (downstream_table_name, '.*\.STAGING_.*_.*', 'i')
		)
), table_exists_in_account AS (
    SELECT
        TABLE_CATALOG
        , TABLE_SCHEMA
        , TABLE_NAME
        , TABLE_TYPE
        , CONCAT(TABLE_CATALOG, '.', TABLE_SCHEMA, '.', TABLE_NAME) AS TABLE_FQDN
    FROM
        SNOWFLAKE.ACCOUNT_USAGE.TABLES
    WHERE
        DELETED IS NULL
), exists_upstream_column_lineage AS (
	SELECT
	    downstream_table_name
		, downstream_column_name
		, array_unique_agg (
			object_construct (
				'upstream_table_name'
				, upstream_object_name
				, 'upstream_column_name'
				, upstream_column_name
			)
		) AS upstream_columns
	FROM
	    column_lineage_history clh
    INNER JOIN
        table_exists_in_account tes
    ON
        clh.upstream_object_name = tes.TABLE_FQDN
    WHERE
        query_exec_time_rank = 1
    GROUP BY
        downstream_table_name
        , downstream_column_name
)
SELECT
    *
FROM
    exists_upstream_column_lineage eucl
INNER JOIN
	table_exists_in_account tes
ON
	eucl.downstream_table_name = tes.TABLE_FQDN
