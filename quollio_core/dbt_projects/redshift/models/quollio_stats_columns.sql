{{
    config(
        materialized='divided_view'
    )
}}
-- depends_on: {{ ref('quollio_stats_profiling_columns') }}
/*
[FIXME]
'macros/materialization/divided_view' is not designed well.
Want to auto generate yaml using dbt-osmosis, so the base query needs to be written here.
*/
