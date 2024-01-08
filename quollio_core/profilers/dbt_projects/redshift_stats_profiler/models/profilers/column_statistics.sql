{% set get_profile_target_columns_query %}
SELECT
    databasename
    , schemaname
    , tablename
    , columnname
    , is_bool
    , is_calculable
FROM
    {{  ref('profile_target_columns')  }}
WHERE
    tablename not in ('column_statistics__dbt_tmp', 'profile_target_columns__dbt_tmp', 'column_statistics', 'profile_target_columns')
    {% for target_table in var("target_tables") -%}
      {%- if loop.first -%}
        and
      {% else -%}
        or
      {% endif -%}
        tablename = '{{target_table}}'
    {% endfor -%}
{% endset %}

{% set results = run_query(get_profile_target_columns_query) %}
{% if execute %}
{% set records = results.rows %}
{% else %}
{% set records = [] %}
{% endif %}

{% for record in records %}
  {% if not loop.first %}
    UNION
  {% endif %}
    SELECT
        '{{record[0]}}' as db_name
        , '{{record[1]}}' as schema_name
        , '{{record[2]}}' as table_name
        , '{{record[3]}}' as column_name
        , {% if record[4] == false %}
            cast(max("{{record[3]}}") as character varying)
          {% else %}
            null
          {% endif %} AS max_value
        , {% if record[4] == false %}
            cast(min("{{record[3]}}") as character varying)
          {% else %}
            null
          {% endif %} AS min_value
        , SUM(NVL2("{{record[3]}}", 0, 1)) AS null_count
        , COUNT(DISTINCT "{{record[3]}}") AS cardinality
        , {% if record[5] == true %}
            avg("{{record[3]}}")
          {% else %}
            null
          {% endif %} AS avg_value
        , {% if record[5] == true %}
            (SELECT
              median("{{record[3]}}")
             FROM 
              {{record[2]}})
          {% else %}
            null
          {% endif %} AS median_value
        , {% if record[4] == false %}
            (SELECT
                cast("{{record[3]}}" as character varying)
             FROM (
                SELECT
                  "{{record[3]}}"
                  , ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS row_num
                FROM
                  {{record[2]}}
                GROUP BY
                  "{{record[3]}}"
              )
              WHERE row_num = 1
            )
          {% else %}
            null
          {% endif %} AS mode_value
        , {% if record[5] == true %}
            cast(stddev("{{record[3]}}") as integer)
          {% else %}
            null
          {% endif %} AS stddev_value
    FROM
      {{ record[0] }}.{{ record[1] }}.{{ record[2] }}
{% endfor %}
