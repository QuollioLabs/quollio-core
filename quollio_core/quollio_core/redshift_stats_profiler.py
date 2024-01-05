import logging
import os
from typing import List

from quollio_core.core import setup
from quollio_core.profilers.stats_profiler import gen_table_stats_payload_from_tuple
from quollio_core.repository import dbt, qdc, redshift

logger = logging.getLogger(__name__)


def redshift_table_stats(
    company_id: str,
    redshift_connections: redshift.RedshiftConnectionConfig,
    qdc_client: qdc.QDCExternalAPIClient,
    target_tables: List[str],
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    with redshift.RedshiftQueryExecutor(config=redshift_connections) as redshift_executor:
        req_count = 0
        for target_table in target_tables:
            stats_query = """
            SELECT
                db_name
                , schema_name
                , table_name
                , column_name
                , max_value
                , min_value
                , null_count
                , cardinality
                , avg_value
                , median_value
                , mode_value
                , stddev_value
            FROM
                {db}.{schema}.column_statistics
            WHERE
                db_name = '{target_db}'
                and schema_name = '{target_schema}'
                and table_name = '{target_table}'
            """.format(
                db=redshift_connections.database,
                schema=redshift_connections.schema,
                target_db=redshift_connections.database,
                target_schema=redshift_connections.schema,
                target_table=target_table,
            )
            stats_result = redshift_executor.get_query_results(query=stats_query)
            payloads = gen_table_stats_payload_from_tuple(
                company_id=company_id, endpoint=redshift_connections.host, stats=stats_result
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
    return


def execute(
    company_id: str,
    redshift_build_view_connections: redshift.RedshiftConnectionConfig,
    qdc_client: qdc.QDCExternalAPIClient,
    is_view_build_only: bool,
    redshift_query_connections: redshift.RedshiftConnectionConfig,
    target_tables: List[str],
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    logger.info("Create stats view")
    dbt_client = dbt.DBTClient()

    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_path = f"{current_dir}/profilers/dbt_projects/redshift_stats_profiler"
    template_path = f"{current_dir}/profilers/templates"
    setup.setup_dbt_profile(
        connections=redshift_build_view_connections,
        project_path=project_path,
        template_path=template_path,
        template_name="redshift_stats_project.j2",
    )
    exec_user_settings = '{{"exec_user": {exec_user}, "target_tables": {target_tables}}}'.format(
        exec_user=redshift_query_connections.user, target_tables=target_tables
    )

    # FIXME: when executing some of the commands, directory changes due to the library bug.
    # https://github.com/dbt-labs/dbt-core/issues/8997
    dbt_client.invoke(
        cmd="deps", project_dir=project_path, profile_dir=project_path, options=["--vars", exec_user_settings]
    )

    dbt_client.invoke(cmd="run", project_dir=".", profile_dir=".", options=["--vars", exec_user_settings])

    if is_view_build_only:
        logger.info("Skip ingesting metadata into QDC.")
        return

    logger.info("Generate redshift stats.")
    redshift_table_stats(
        company_id=company_id,
        redshift_connections=redshift_query_connections,
        qdc_client=qdc_client,
        target_tables=target_tables,
    )

    logger.info("redshift stats profiler is successfully finished.")
    return
