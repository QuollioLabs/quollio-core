{% set get_profile_target_columns_query %}
SELECT
    *
FROM
    {{  ref('profile_target_columns')  }}
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
        , CAST(max("{{record[3]}}") AS STRING) AS max_value
        , CAST(min("{{record[3]}}") AS STRING) AS min_value
        , COUNT_IF("{{record[3]}}" IS NULL) AS null_count
        , APPROX_COUNT_DISTINCT("{{record[3]}}") AS cardinality
        , {% if record[5] == true %}
            avg("{{record[3]}}")
          {% else %}
            null
          {% endif %} AS avg_value
        , {% if record[5] == true %}
            median("{{record[3]}}")
          {% else %}
            null
          {% endif %} AS median_value
        , {% if record[5] == true %}
            approx_top_k("{{record[3]}}")[0][0]
          {% else %}
            null
          {% endif %} AS mode_value
        , {% if record[5] == true %}
            stddev("{{record[3]}}")
          {% else %}
            null
          {% endif %} AS stddev_value
    FROM
      {{ record[0] }}.{{ record[1] }}.{{ record[2] }} {{ var("sample_method") }}
{% endfor %}
