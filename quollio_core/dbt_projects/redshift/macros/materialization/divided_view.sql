{%- materialization divided_view, default %}
{%- set identifier = model['alias'] %}
{%- set target_relations = [] %}
{%- set grant_config = config.get('grants') %}

{{ run_hooks(pre_hooks, inside_transaction=False) }}
-- `BEGIN` happens here:
{{ run_hooks(pre_hooks, inside_transaction=True) }}

-- fetch target_tables
{%- set query_stats_target_tables -%}
    SELECT
      distinct
      database_name
      , schema_name
      , table_name
    FROM
      {{ ref('quollio_stats_profiling_columns') }}
    WHERE
      table_name not like 'quollio_%%'
{%- endset -%}
{%- set results = run_query(query_stats_target_tables) -%}
{%- if execute -%}
{%- set stats_target_tables = results.rows -%}
{%- else -%}
{%- set stats_target_tables = [] -%}
{%- endif -%}

-- skip creating views if the target profiling columns don't exist.
{%- if stats_target_tables | length == 0 -%}
  {% call statement("main") %}
    {{ log("No records found. Just execute select stmt for skipping call statement.", info=True) }}
    select null
  {% endcall %}
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {%- set should_revoke = should_revoke(target_relation, full_refresh_mode) %}
{%- endif -%}

-- build sql
{%- for stats_target_table in stats_target_tables -%}
  -- get columns for statistics. 
  -- LISTAGG function can't be used for sys table, then it's necessary to get column for each table. 
  -- See https://docs.aws.amazon.com/redshift/latest/dg/c_join_PG.html.
  {%- set stats_target_columns %}
      SELECT
        database_name
        , schema_name
        , table_name
        , column_name
        , is_bool
        , is_calculable
      FROM
        {{ ref('quollio_stats_profiling_columns') }}
      WHERE
        database_name = '{{stats_target_table[0]}}'
        AND schema_name = '{{stats_target_table[1]}}'
        AND table_name = '{{stats_target_table[2]}}'
  {%- endset -%}

  {%- set results = run_query(stats_target_columns) -%}
  {%- set stats_target_columns = results.rows -%}

  {%- set sql_for_column_stats %}
  {%- for stats_target_column in stats_target_columns -%}
    {%- if not loop.first -%}UNION{% endif %}
    SELECT
      main.db_name
      , main.schema_name
      , main.table_name
      , main.column_name
      , main.max_value
      , main.min_value
      , main.null_count
      , main.cardinality
      , main.avg_value
      , main.median_value
      , mode.mode_value
      , main.stddev_value
    FROM
      (
      SELECT
        DISTINCT
        '{{stats_target_column[0]}}'::varchar as db_name
        , '{{stats_target_column[1]}}'::varchar as schema_name
        , '{{stats_target_column[2]}}'::varchar as table_name
        , '{{stats_target_column[3]}}'::varchar as column_name
        , {% if var("aggregate_all") == True and stats_target_column[5] == True %}cast(max("{{stats_target_column[3]}}") as varchar){% else %}null::varchar{% endif %} AS max_value
        , {% if var("aggregate_all") == True and stats_target_column[5] == True %}cast(min("{{stats_target_column[3]}}") as varchar){% else %}null::varchar{% endif %} AS min_value
        -- requires full table scan
        , {% if var("aggregate_all") == True %}cast(SUM(NVL2("{{stats_target_column[3]}}", 0, 1)) as integer){% else %}null::integer{% endif %} AS null_count
        , APPROXIMATE COUNT(DISTINCT "{{stats_target_column[3]}}") AS cardinality
        -- requires full table scan
        , {% if var("aggregate_all") == True and stats_target_column[5] == True %}cast(avg("{{stats_target_column[3]}}")as varchar){% else %}null::varchar{% endif %} AS avg_value
        , {% if var("aggregate_all") == True and stats_target_column[5] == True %}cast(median("{{stats_target_column[3]}}") as varchar){% else %}null::varchar{% endif %} AS median_value
        -- requires full table scan
        , {% if stats_target_column[5] == True %}cast(STDDEV_SAMP("{{stats_target_column[3]}}") as integer){% else %}null::integer{% endif %} AS stddev_value
      FROM {{ stats_target_column[0] }}.{{ stats_target_column[1] }}.{{ stats_target_column[2] }}
    ) main, (
      {%- if var("aggregate_all") == True and stats_target_column[4] == false %}
        SELECT
          cast("{{stats_target_column[3]}}" as varchar) mode_value
        FROM (
           SELECT
            DISTINCT
            "{{stats_target_column[3]}}"
            , ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS row_num
          FROM {{ stats_target_column[0] }}.{{ stats_target_column[1] }}.{{ stats_target_column[2] }}
          GROUP BY
            "{{stats_target_column[3]}}"
        )
        WHERE
          row_num = 1
      {% else %}
        SELECT null as mode_value {%- endif -%}
    ) mode
  {% endfor -%}
  {%- endset %}
  -- create a view with a index as suffix
  {%- set target_identifier = "%s_%s_%s_%s"|format(model['name'], stats_target_table[0], stats_target_table[1], stats_target_table[2]) %}
  {%- set target_relation = api.Relation.create(identifier=target_identifier, schema=schema, database=database, type='view') %}
  -- {{ drop_relation_if_exists(target_relation) }}
  {% call statement("main") %}
    {{ get_replace_view_sql(target_relation, sql_for_column_stats) }}
  {% endcall %}
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {%- set should_revoke = should_revoke(target_relation, full_refresh_mode) %}
  {%- do apply_grants(target_relation, grant_config, should_revoke) %}
  {%- set target_relations = target_relations.append(target_relation) %}
{%- endfor -%}

{{ run_hooks(post_hooks, inside_transaction=True) }}
{{ adapter.commit() }}
{{ run_hooks(post_hooks, inside_transaction=False) }}

{{ return({'relations': target_relations}) }}
{%- endmaterialization -%}
