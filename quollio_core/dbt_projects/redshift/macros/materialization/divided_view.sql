{%- materialization divided_view, default %}
{%- set identifier = model['alias'] %}
{%- set target_relations = [] %}
{%- set chunk = config.get('chunk') %}
{%- set grant_config = config.get('grants') %}

{{ run_hooks(pre_hooks, inside_transaction=False) }}
-- `BEGIN` happens here:
{{ run_hooks(pre_hooks, inside_transaction=True) }}

-- fetch records
{%- set query_quollio_stats_profiling_columns -%}
SELECT * FROM {{  ref('quollio_stats_profiling_columns')  }} WHERE table_name not like 'quollio_%'
{%- endset -%}
{%- set results = run_query(query_quollio_stats_profiling_columns) -%}
{%- if execute -%}
{%- set records = results.rows -%}
{%- else -%}
{%- set records = [] -%}
{%- endif -%}

-- build sql
{%- for i in range(0, records|length, chunk) -%}
  {%- set build_sql %}
  {%- for record in records[i: i+chunk] -%}
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
        '{{record[0]}}'::varchar as db_name
        , '{{record[1]}}'::varchar as schema_name
        , '{{record[2]}}'::varchar as table_name
        , '{{record[3]}}'::varchar as column_name
        , {% if var("skip_heavy") == false and record[5] == true %}cast(max("{{record[3]}}") as varchar){% else %}null::varchar{% endif %} AS max_value
        , {% if var("skip_heavy") == false and record[5] == true %}cast(min("{{record[3]}}") as varchar){% else %}null::varchar{% endif %} AS min_value
        -- requires full table scan
        , {% if var("skip_heavy") == false %}cast(SUM(NVL2("{{record[3]}}", 0, 1)) as integer){% else %}null::integer{% endif %} AS null_count
        , APPROXIMATE COUNT(DISTINCT "{{record[3]}}") AS cardinality
        -- requires full table scan
        , {% if var("skip_heavy") == false and record[5] == true %}cast(avg("{{record[3]}}")as varchar){% else %}null::varchar{% endif %} AS avg_value
        , {% if var("skip_heavy") == false and record[5] == true %}cast(median("{{record[3]}}") as varchar){% else %}null::varchar{% endif %} AS median_value
        -- requires full table scan
        , {% if record[5] == true %}cast(STDDEV_SAMP("{{record[3]}}") as integer){% else %}null::integer{% endif %} AS stddev_value
      FROM {{ record[0] }}.{{ record[1] }}.{{ record[2] }}
    ) main, (
      {%- if var("skip_heavy") == false and record[4] == false %}
        SELECT
          cast("{{record[3]}}" as varchar) mode_value
        FROM (
           SELECT
            DISTINCT
            "{{record[3]}}"
            , ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS row_num
          FROM {{ record[0] }}.{{ record[1] }}.{{ record[2] }}
          GROUP BY
            "{{record[3]}}"
        )
        WHERE
          row_num = 1
      {% else %}
        SELECT null as mode_value {%- endif -%}
    ) mode
  {% endfor -%}
  {%- endset %}
  -- create a view with a index as suffix
  {%- set target_identifier = "%s_%d"|format(model['name'], loop.index) %}
  {%- set target_relation = api.Relation.create(identifier=target_identifier, schema=schema, database=database, type='view') %}
  -- {{ drop_relation_if_exists(target_relation) }}
  {% call statement("main") %}
    {{ get_replace_view_sql(target_relation, build_sql) }}
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
