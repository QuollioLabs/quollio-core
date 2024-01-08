import logging
import os
from typing import List

from quollio_core.core import setup
from quollio_core.profilers.stats_profiler import gen_table_stats_payload
from quollio_core.repository import dbt, qdc, snowflake

logger = logging.getLogger(__name__)


def _build_query(target_databases: list) -> str:
    if len(target_databases) == 0:
        query = """
            SELECT
                DISTINCT
                TABLE_CATALOG
                , TABLE_SCHEMA
                , TABLE_NAME
            FROM
                QUOLLIO_DATA_PROFILER.PUBLIC.PROFILE_TARGET_COLUMNS
            """
    else:
        db_str = ", ".join("'" + db + "'" for db in target_databases)
        query = """
            SELECT
                DISTINCT
                TABLE_CATALOG
                , TABLE_SCHEMA
                , TABLE_NAME
            FROM
                QUOLLIO_DATA_PROFILER.PUBLIC.PROFILE_TARGET_COLUMNS
            WHERE
                TABLE_CATALOG in({databases})
            """.format(
            databases=db_str
        )
    return query


def snowflake_table_stats(
    company_id: str,
    snowflake_connections: snowflake.SnowflakeConnectionConfig,
    qdc_client: qdc.QDCExternalAPIClient,
    target_databases: list = [],
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    sf_executor = snowflake.SnowflakeQueryExecutor(snowflake_connections, "QUOLLIO_DATA_PROFILER", "PUBLIC")

    query = _build_query(target_databases=target_databases)
    target_assets = sf_executor.get_query_results(query=query)
    req_count = 0
    for target_asset in target_assets:
        stats_query = """
        SELECT
            *
        FROM
            QUOLLIO_DATA_PROFILER.PUBLIC.COLUMN_STATISTICS
        WHERE
            DB_NAME = '{db}'
            AND SCHEMA_NAME = '{schema}'
            AND TABLE_NAME = '{table}'
            AND DB_NAME != 'QUOLLIO_DATA_PROFILER'
        """.format(
            db=target_asset["TABLE_CATALOG"], schema=target_asset["TABLE_SCHEMA"], table=target_asset["TABLE_NAME"]
        )
        stats_result = sf_executor.get_query_results(query=stats_query)
        payloads = gen_table_stats_payload(
            company_id=company_id, endpoint=snowflake_connections.account_id, stats=stats_result
        )
        for payload in payloads:
            logger.info(
                "Generating table stats. asset: {db} -> {schema} -> {table} -> {column}".format(
                    db=payload.db,
                    schema=payload.schema,
                    table=payload.table,
                    column=payload.column,
                )
            )
            status_code = qdc_client.update_stats_by_id(
                global_id=payload.global_id,
                payload=payload.body.get_column_stats(),
            )
            if status_code == 200:
                req_count += 1
    logger.info(f"Generating table stats is finished. {req_count} stats are ingested.")


def execute(
    company_id: str,
    sf_build_view_connections: snowflake.SnowflakeConnectionConfig,
    qdc_client: qdc.QDCExternalAPIClient,
    is_view_build_only: bool,
    sf_query_connections: snowflake.SnowflakeConnectionConfig,
    stats_target_databases: List[str] = [],
    stats_sample_method: str = "SAMPLE(10)",
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    logger.info("Create stats view")
    dbt_client = dbt.DBTClient()

    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_path = f"{current_dir}/profilers/dbt_projects/snowflake_stats_profiler"
    template_path = f"{current_dir}/profilers/templates"
    setup.setup_dbt_profile(
        connections=sf_build_view_connections,
        project_path=project_path,
        template_path=template_path,
        template_name="snowflake_stats_project.j2",
    )
    var_settings = '{{"exec_role": {exec_role}, "sample_method": {sample_method}}}'.format(
        exec_role=sf_query_connections.account_role, sample_method=stats_sample_method
    )

    # FIXME: when executing some of the commands, directory changes due to the library bug.
    # https://github.com/dbt-labs/dbt-core/issues/8997
    dbt_client.invoke(cmd="deps", project_dir=project_path, profile_dir=project_path, options=["--vars", var_settings])
    dbt_client.invoke(cmd="run", project_dir=".", profile_dir=".", options=["--vars", var_settings])

    if is_view_build_only:
        logger.info("Skip ingesting metadata into QDC.")
        return

    snowflake_table_stats(
        company_id=company_id,
        snowflake_connections=sf_query_connections,
        qdc_client=qdc_client,
        target_databases=stats_target_databases,
    )
