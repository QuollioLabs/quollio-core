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
SELECT * FROM {{  ref('quollio_stats_profiling_columns')  }} WHERE NOT startswith(table_name, 'QUOLLIO_')
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
    {%- if not loop.first %}UNION{% endif %}
    SELECT
      DISTINCT
      '{{record[0]}}' as db_name
      , '{{record[1]}}' as schema_name
      , '{{record[2]}}' as table_name
      , '{{record[3]}}' as column_name
      , {% if record[5] == true %}CAST(max("{{record[3]}}") AS STRING){% else %}null{% endif %} AS max_value
      , {% if record[5] == true %}CAST(min("{{record[3]}}") AS STRING){% else %}null{% endif %} AS min_value
      , COUNT_IF("{{record[3]}}" IS NULL) AS null_count
      , APPROX_COUNT_DISTINCT("{{record[3]}}") AS cardinality
      , {% if record[5] == true %}avg("{{record[3]}}"){% else %}null{% endif %} AS avg_value
      , {% if record[5] == true %}median("{{record[3]}}"){% else %}null{% endif %} AS median_value
      , {% if record[5] == true %}approx_top_k("{{record[3]}}")[0][0]{% else %}null{% endif %} AS mode_value
      , {% if record[5] == true %}stddev("{{record[3]}}"){% else %}null{% endif %} AS stddev_value
    FROM {{ record[0] }}.{{ record[1] }}.{{ record[2] }} {{ var("sample_method") }}
  {% endfor -%}
  {%- endset %}
  -- create a view with a index as suffix
  {%- set target_identifier = "%s_%d"|format(model['name'], loop.index) %}
  {%- set target_relation = api.Relation.create(identifier=target_identifier, schema=schema, database=database, type='view') %}
  {% call statement("main") %}
    {{ get_create_view_as_sql(target_relation, build_sql) }}
  {% endcall %}
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {%- set should_revoke = should_revoke(target_relation, full_refresh_mode) %}
  {%- do apply_grants(target_relation, grant_config, should_revoke) %}
  {%- set target_relations = target_relations.append(target_relation) %}
{%- endfor -%}

{{ run_hooks(post_hooks, inside_transaction=True) }}
-- `COMMIT` happens here:
{{ adapter.commit() }}
{{ run_hooks(post_hooks, inside_transaction=False) }}

{{ return({'relations': target_relations}) }}
{%- endmaterialization -%}
