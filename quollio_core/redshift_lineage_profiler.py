import logging
import os

from quollio_core.core import setup
from quollio_core.profilers.lineage_profiler import gen_table_lineage_payload, gen_table_lineage_payload_inputs
from quollio_core.repository import dbt, qdc, redshift

logger = logging.getLogger(__name__)


def redshift_table_level_lineage(
    company_id: str,
    redshift_connections: redshift.RedshiftConnectionConfig,
    qdc_client: qdc.QDCExternalAPIClient,
    dbt_table_name: str,
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    with redshift.RedshiftQueryExecutor(config=redshift_connections) as redshift_executor:
        results = redshift_executor.get_query_results(
            query="""
            SELECT
                *
            FROM
                {db}.{schema}.{table}
            """.format(
                db=redshift_connections.database, schema=redshift_connections.schema, table=dbt_table_name
            )
        )
        lineage_payload_inputs = gen_table_lineage_payload_inputs(input_data=results)

        update_table_lineage_inputs = gen_table_lineage_payload(
            company_id=company_id,
            endpoint=redshift_connections.host,
            tables=lineage_payload_inputs,
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


def execute(
    company_id: str,
    redshift_build_view_connections: redshift.RedshiftConnectionConfig,
    qdc_client: qdc.QDCExternalAPIClient,
    is_view_build_only: bool,
    redshift_query_connections: redshift.RedshiftConnectionConfig,
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    logger.info("Create lineage view")
    dbt_client = dbt.DBTClient()

    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_path = f"{current_dir}/profilers/dbt_projects/redshift_lineage_profiler"
    template_path = f"{current_dir}/profilers/templates"
    setup.setup_dbt_profile(
        connections=redshift_build_view_connections,
        project_path=project_path,
        template_path=template_path,
        template_name="redshift_lineage_project.j2",
    )
    exec_user_settings = '{{"exec_user": {exec_user}}}'.format(exec_user=redshift_query_connections.user)

    # FIXME: when executing some of the commands, directory changes due to the library bug.
    # https://github.com/dbt-labs/dbt-core/issues/8997
    dbt_client.invoke(
        cmd="deps", project_dir=project_path, profile_dir=project_path, options=["--vars", exec_user_settings]
    )

    dbt_client.invoke(cmd="run", project_dir=".", profile_dir=".", options=["--vars", exec_user_settings])

    if is_view_build_only:
        logger.info("Skip ingesting metadata into QDC.")
        return

    logger.info("Generate redshift table to table lineage.")
    redshift_table_level_lineage(
        company_id=company_id,
        redshift_connections=redshift_query_connections,
        qdc_client=qdc_client,
        dbt_table_name="table_to_table_lineage",
    )

    logger.info("Generate redshift view level lineage.")
    redshift_table_level_lineage(
        company_id=company_id,
        redshift_connections=redshift_query_connections,
        qdc_client=qdc_client,
        dbt_table_name="view_level_lineage",
    )

    logger.info("redshift lineage profiler is successfully finished.")
    return
