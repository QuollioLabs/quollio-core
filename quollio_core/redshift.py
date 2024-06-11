import argparse
import logging
import os

from quollio_core.helper.core import setup_dbt_profile
from quollio_core.helper.env_default import env_default
from quollio_core.profilers.redshift import (
    redshift_table_level_lineage,
    redshift_table_level_sqllineage,
    redshift_table_stats,
)
from quollio_core.repository import dbt, qdc, redshift

logger = logging.getLogger(__name__)


def build_view(
    conn: redshift.RedshiftConnectionConfig,
    aggregate_all: bool = False,
    target_tables: str = "",
    log_level: str = "info",
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    logger.info("Build profiler views using dbt")
    # set parameters
    dbt_client = dbt.DBTClient()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_path = f"{current_dir}/dbt_projects/redshift"
    template_path = f"{current_dir}/dbt_projects/redshift/profiles"
    template_name = "profiles_template.yml"
    options = '{{"query_user": {query_user}, "aggregate_all": {aggregate_all}, "target_database": {database}}}'.format(
        query_user=conn.query_user,
        aggregate_all=aggregate_all,
        database=conn.database,
    )

    # build views using dbt
    setup_dbt_profile(
        connections_json=conn.as_dict(),
        template_path=template_path,
        template_name=template_name,
    )
    # FIXME: when executing some of the commands, directory changes due to the library bug.
    # https://github.com/dbt-labs/dbt-core/issues/8997
    dbt_client.invoke(
        cmd="deps",
        project_dir=project_path,
        profile_dir=template_path,
        options=["--no-use-colors", "--log-level", log_level, "--vars", options],
    )
    run_options = ["--no-use-colors", "--log-level", log_level, "--vars", options]
    if target_tables is not None:
        if "quollio_stats_columns" in target_tables:
            target_tables.append("quollio_stats_profiling_columns")
        target_tables_str = " ".join(target_tables)
        run_options.append("--select")
        run_options.append(target_tables_str)

    dbt_client.invoke(
        cmd="run",
        project_dir=project_path,
        profile_dir=template_path,
        options=run_options,
    )

    logger.info("Profiler views are successfully built.")

    return


def load_lineage(
    conn: redshift.RedshiftConnectionConfig,
    qdc_client: qdc.QDCExternalAPIClient,
    tenant_id: str,
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    logger.info("Generate redshift table to table lineage.")
    redshift_table_level_lineage(
        conn=conn,
        qdc_client=qdc_client,
        tenant_id=tenant_id,
        dbt_table_name="quollio_lineage_table_level",
    )

    logger.info("Generate redshift view level lineage.")
    redshift_table_level_lineage(
        conn=conn,
        qdc_client=qdc_client,
        tenant_id=tenant_id,
        dbt_table_name="quollio_lineage_view_level",
    )

    logger.info("Lineage data is successfully loaded.")

    return


def load_stats(
    conn: redshift.RedshiftConnectionConfig,
    qdc_client: qdc.QDCExternalAPIClient,
    tenant_id: str,
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    logger.info("Generate redshift stats.")
    redshift_table_stats(
        conn=conn,
        qdc_client=qdc_client,
        tenant_id=tenant_id,
    )

    logger.info("Stats data is successfully loaded.")
    return


def load_sqllineage(
    conn: redshift.RedshiftConnectionConfig,
    qdc_client: qdc.QDCExternalAPIClient,
    tenant_id: str,
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    logger.info("Generate Redshift sqllineage.")
    redshift_table_level_sqllineage(
        conn=conn,
        qdc_client=qdc_client,
        tenant_id=tenant_id,
    )

    logger.info("sqllineage data is successfully loaded.")

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Quollio Intelligence Agent for Redshift",
        description="Build views and load lineage and stats to Quollio from Redshift using dbt.",
        epilog="Copyright (c) 2024 Quollio Technologies, Inc.",
    )
    parser.add_argument(
        "commands",
        choices=["build_view", "load_lineage", "load_stats", "load_sqllineage"],
        type=str,
        nargs="+",
        help="""
        The command to execute.
        'build_view': Build views using dbt,
        'load_lineage': Load lineage data from created views to Quollio,
        'load_stats': Load stats from created views to Quollio,
        'load_sqllineage': Load lineage data from sql parse result(alpha),
        """,
    )
    parser.add_argument(
        "--host",
        type=str,
        action=env_default("REDSHIFT_HOST"),
        required=False,
        help="Host of Redshift cluster",
    )
    parser.add_argument(
        "--port",
        type=int,
        action=env_default("REDSHIFT_PORT"),
        default=5439,
        required=False,
        help="Port of Redshift cluster",
    )
    parser.add_argument(
        "--build_user",
        type=str,
        action=env_default("REDSHIFT_BUILD_USER"),
        required=False,
        help="User name that is used to build views by dbt",
    )
    parser.add_argument(
        "--query_user",
        type=str,
        action=env_default("REDSHIFT_QUERY_USER"),
        required=False,
        help="User name that is used to query views",
    )
    parser.add_argument(
        "--build_password",
        type=str,
        action=env_default("REDSHIFT_BUILD_PASSWORD"),
        required=False,
        help="User password is used to build views by dbt",
    )
    parser.add_argument(
        "--query_password",
        type=str,
        action=env_default("REDSHIFT_QUERY_PASSWORD"),
        required=False,
        help="User password is used to query views",
    )
    parser.add_argument(
        "--database",
        type=str,
        action=env_default("REDSHIFT_TARGET_DATABASE"),
        help="Target database name where the views are built by dbt",
    )
    parser.add_argument(
        "--schema",
        type=str,
        action=env_default("REDSHIFT_TARGET_SCHEMA"),
        default="public",
        required=False,
        help="Target schema name where the views are built by dbt",
    )
    parser.add_argument(
        "--aggregate_all",
        type=bool,
        action=env_default("REDSHIFT_AGGREGATE_ALL", store_true=True),
        default=False,
        required=False,
        help="Aggregate all stats values. False by default.",
    )
    parser.add_argument(
        "--target_tables",
        type=str,
        nargs="*",
        choices=["quollio_lineage_table_level", "quollio_lineage_view_level", "quollio_stats_columns"],
        action=env_default("REDSHIFT_TARGET_TABLES"),
        required=False,
        help="Target tables you want to create with dbt module. \
              You need to specify this parameter if you want to specify tables, not all ones. \
              Please specify table name with blank delimiter like tableA tableB \
              if you want to create two or more tables",
    )
    parser.add_argument(
        "--tenant_id",
        type=str,
        action=env_default("TENANT_ID"),
        required=False,
        help="The tenant id (company id) where the lineage and stats are loaded",
    )
    parser.add_argument(
        "--log_level",
        type=str,
        choices=["debug", "info", "warn", "error", "none"],
        action=env_default("LOG_LEVEL"),
        required=False,
        help="The log level for dbt commands. Default value is info",
    )
    parser.add_argument(
        "--api_url",
        type=str,
        action=env_default("QDC_API_URL"),
        required=False,
        help="The base URL of Quollio External API",
    )
    parser.add_argument(
        "--client_id",
        type=str,
        action=env_default("QDC_CLIENT_ID"),
        required=False,
        help="The client id that is created on Quollio console to let clients access Quollio External API",
    )
    parser.add_argument(
        "--client_secret",
        type=str,
        action=env_default("QDC_CLIENT_SECRET"),
        required=False,
        help="The client secrete that is created on Quollio console to let clients access Quollio External API",
    )
    args = parser.parse_args()
    conn = redshift.RedshiftConnectionConfig(
        host=args.host,
        build_user=args.build_user,
        query_user=args.query_user,
        build_password=args.build_password,
        query_password=args.query_password,
        database=args.database,
        schema=args.schema,
        port=args.port,
    )

    if len(args.commands) == 0:
        raise ValueError("No command is provided")

    if "build_view" in args.commands:
        build_view(
            conn=conn,
            aggregate_all=args.aggregate_all,
            target_tables=args.target_tables,
            log_level=args.log_level,
        )
    if "load_lineage" in args.commands:
        qdc_client = qdc.QDCExternalAPIClient(
            client_id=args.client_id,
            client_secret=args.client_secret,
            base_url=args.api_url,
        )
        load_lineage(
            conn=conn,
            qdc_client=qdc_client,
            tenant_id=args.tenant_id,
        )
    if "load_stats" in args.commands:
        qdc_client = qdc.QDCExternalAPIClient(
            client_id=args.client_id,
            client_secret=args.client_secret,
            base_url=args.api_url,
        )
        load_stats(
            conn=conn,
            qdc_client=qdc_client,
            tenant_id=args.tenant_id,
        )
    if "load_sqllineage" in args.commands:
        qdc_client = qdc.QDCExternalAPIClient(
            base_url=args.api_url,
            client_id=args.client_id,
            client_secret=args.client_secret,
        )
        load_sqllineage(
            conn=conn,
            qdc_client=qdc_client,
            tenant_id=args.tenant_id,
        )
