import logging
import os

from quollio_core.core import setup
from quollio_core.profilers.lineage_profiler import (
    gen_column_lineage_payload,
    gen_table_lineage_payload,
    parse_snowflake_results,
)
from quollio_core.repository import dbt, qdc, snowflake

logger = logging.getLogger(__name__)


def snowflake_table_to_table_lineage(
    company_id: str, snowflake_connections: snowflake.SnowflakeConnectionConfig, qdc_client: qdc.QDCExternalAPIClient
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    sf_executor = snowflake.SnowflakeQueryExecutor(snowflake_connections, "QUOLLIO_DATA_PROFILER", "PUBLIC")
    results = sf_executor.get_query_results(
        query="""
        SELECT
            *
        FROM
            QUOLLIO_DATA_PROFILER.PUBLIC.TABLE_TO_TABLE_LINEAGE
        """
    )
    parsed_results = parse_snowflake_results(results=results)
    update_table_lineage_inputs = gen_table_lineage_payload(
        company_id=company_id,
        endpoint=snowflake_connections.account_id,
        tables=parsed_results,
    )

    req_count = 0
    for update_table_lineage_input in update_table_lineage_inputs:
        logger.info(
            "Generating table lineage. downstream: {db} -> {schema} -> {table}".format(
                db=update_table_lineage_input.downstream_database_name,
                schema=update_table_lineage_input.downstream_schema_name,
                table=update_table_lineage_input.downstream_table_name,
            )
        )
        status_code = qdc_client.update_lineage_by_id(
            global_id=update_table_lineage_input.downstream_global_id,
            payload=update_table_lineage_input.upstreams.as_dict(),
        )
        if status_code == 200:
            req_count += 1
    logger.info(f"Generating table lineage is finished. {req_count} lineages are ingested.")
    return


def snowflake_column_to_column_lineage(
    company_id: str, snowflake_connections: snowflake.SnowflakeConnectionConfig, qdc_client: qdc.QDCExternalAPIClient
) -> None:
    sf_executor = snowflake.SnowflakeQueryExecutor(snowflake_connections, "QUOLLIO_DATA_PROFILER", "PUBLIC")
    results = sf_executor.get_query_results(
        query="""
        SELECT
            *
        FROM
            QUOLLIO_DATA_PROFILER.PUBLIC.COLUMN_TO_COLUMN_LINEAGE
        """
    )
    update_column_lineage_inputs = gen_column_lineage_payload(
        company_id=company_id,
        endpoint=snowflake_connections.account_id,
        columns=results,
    )

    req_count = 0
    for update_column_lineage_input in update_column_lineage_inputs:
        logger.info(
            "Generating column lineage. downstream: {db} -> {schema} -> {table} -> {column}".format(
                db=update_column_lineage_input.downstream_database_name,
                schema=update_column_lineage_input.downstream_schema_name,
                table=update_column_lineage_input.downstream_table_name,
                column=update_column_lineage_input.downstream_column_name,
            )
        )
        status_code = qdc_client.update_lineage_by_id(
            global_id=update_column_lineage_input.downstream_global_id,
            payload=update_column_lineage_input.upstreams.as_dict(),
        )
        if status_code == 200:
            req_count += 1
    logger.info(f"Generating column lineage is finished. {req_count} lineages are ingested.")
    return


def execute(
    company_id: str,
    sf_build_view_connections: snowflake.SnowflakeConnectionConfig,
    qdc_client: qdc.QDCExternalAPIClient,
    is_view_build_only: bool,
    sf_query_connections: snowflake.SnowflakeConnectionConfig,
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    logger.info("Create lineage view")
    dbt_client = dbt.DBTClient()

    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_path = f"{current_dir}/profilers/dbt_projects/snowflake_lineage_profiler"
    template_path = f"{current_dir}/profilers/templates"
    setup.setup_dbt_profile(
        connections=sf_build_view_connections,
        project_path=project_path,
        template_path=template_path,
        template_name="snowflake_lineage_project.j2",
    )
    exec_role_settings = '{{"exec_role": {exec_role}}}'.format(exec_role=sf_query_connections.account_role)

    # FIXME: when executing some of the commands, directory changes due to the library bug.
    # https://github.com/dbt-labs/dbt-core/issues/8997
    dbt_client.invoke(
        cmd="deps", project_dir=project_path, profile_dir=project_path, options=["--vars", exec_role_settings]
    )

    dbt_client.invoke(cmd="run", project_dir=".", profile_dir=".", options=["--vars", exec_role_settings])

    if is_view_build_only:
        logger.info("Skip ingesting metadata into QDC.")
        return

    logger.info("Generate snowflake table to table lineage.")
    snowflake_table_to_table_lineage(
        company_id=company_id, snowflake_connections=sf_query_connections, qdc_client=qdc_client
    )

    logger.info("Generate snowflake column to column lineage.")
    snowflake_column_to_column_lineage(
        company_id=company_id, snowflake_connections=sf_query_connections, qdc_client=qdc_client
    )

    logger.info("Snowflake lineage profiler is successfully finished.")
    return
