import argparse
import logging
import os

from quollio_core.helper.core import setup_dbt_profile
from quollio_core.helper.env_default import env_default
from quollio_core.profilers.snowflake import (
    snowflake_column_to_column_lineage,
    snowflake_table_stats,
    snowflake_table_to_table_lineage,
)
from quollio_core.repository import dbt, qdc, snowflake

logger = logging.getLogger(__name__)


def build_view(
    conn: snowflake.SnowflakeConnectionConfig,
    stats_sample_method: str,
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    logger.info("Build profiler views using dbt")
    # set parameters
    dbt_client = dbt.DBTClient()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_path = f"{current_dir}/dbt_projects/snowflake"
    template_path = f"{current_dir}/dbt_projects/snowflake/profiles"
    template_name = "profiles_template.yml"
    options = '{{"query_role": {query_role}, "sample_method": {sample_method}}}'.format(
        query_role=conn.account_query_role,
        sample_method=stats_sample_method,
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
        options=["--no-use-colors", "--vars", options],
    )
    dbt_client.invoke(
        cmd="run",
        project_dir=project_path,
        profile_dir=template_path,
        options=["--no-use-colors", "--vars", options],
    )

    logger.info("Profiler views are successfully built.")
    return


def load_lineage(
    conn: snowflake.SnowflakeConnectionConfig,
    qdc_client: qdc.QDCExternalAPIClient,
    tenant_id: str,
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    logger.info("Generate Snowflake table to table lineage.")
    snowflake_table_to_table_lineage(
        conn=conn,
        qdc_client=qdc_client,
        tenant_id=tenant_id,
    )

    logger.info("Generate Snowflake column to column lineage.")
    snowflake_column_to_column_lineage(
        conn=conn,
        qdc_client=qdc_client,
        tenant_id=tenant_id,
    )

    logger.info("Lineage data is successfully loaded.")

    return


def load_stats(
    conn: snowflake.SnowflakeConnectionConfig,
    qdc_client: qdc.QDCExternalAPIClient,
    tenant_id: str,
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    logger.info("Generate Snowflake stats.")
    snowflake_table_stats(
        conn=conn,
        qdc_client=qdc_client,
        tenant_id=tenant_id,
    )

    logger.info("Stats data is successfully loaded.")

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Quollio Intelligence Agent for Snowflake",
        description="Build views and load lineage and stats to Quollio from Snowflake using dbt.",
        epilog="Copyright (c) 2024 Quollio Technologies, Inc.",
    )
    parser.add_argument(
        "commands",
        choices=["build_view", "load_lineage", "load_stats"],
        type=str,
        nargs="+",
        help="""
        The command to execute.
        'build_view': Build views using dbt,
        'load_lineage': Load lineage data from created views to Quollio,
        'load_stats': Load stats from created views to Quollio
        """,
    )
    parser.add_argument(
        "--account_id",
        type=str,
        action=env_default("SNOWFLAKE_ACCOUNT_ID"),
        required=False,
        help="The target account ID of Snowflake",
    )
    parser.add_argument(
        "--user",
        type=str,
        action=env_default("SNOWFLAKE_USER"),
        required=False,
        help="User name to connect to the database",
    )
    parser.add_argument(
        "--password",
        type=str,
        action=env_default("SNOWFLAKE_PASSWORD"),
        required=False,
        help="User password to connect to the database",
    )
    parser.add_argument(
        "--build_role",
        type=str,
        action=env_default("SNOWFLAKE_BUILD_ROLE"),
        required=False,
        help="The role in Snowflake that is used to build views by dbt",
    )
    parser.add_argument(
        "--query_role",
        type=str,
        action=env_default("SNOWFLAKE_QUERY_ROLE"),
        required=False,
        help="The role in Snowflake that is used to query views",
    )
    parser.add_argument(
        "--warehouse",
        type=str,
        action=env_default("SNOWFLAKE_WAREHOUSE"),
        required=False,
        help="The warehouse in Snowflake that is used to execute statements",
    )
    parser.add_argument(
        "--database",
        type=str,
        action=env_default("SNOWFLAKE_TARGET_DATABASE"),
        required=False,
        help="Target database name where the views are built by dbt",
    )
    parser.add_argument(
        "--schema",
        type=str,
        action=env_default("SNOWFLAKE_TARGET_SCHEMA"),
        default="public",
        required=False,
        help="Target schema name where the views are built by dbt",
    )
    parser.add_argument(
        "--sample_method",
        type=str,
        action=env_default("SNOWFLAKE_SAMPLE_METHOD"),
        default="SAMPLE(10)",
        required=False,
        help="The method to sample data for stats",
    )
    parser.add_argument(
        "--tenant_id",
        type=str,
        action=env_default("TENANT_ID"),
        required=False,
        help="The tenant id (company id) where the lineage and stats are loaded",
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
    conn = snowflake.SnowflakeConnectionConfig(
        account_id=args.account_id,
        account_user=args.user,
        account_password=args.password,
        account_build_role=args.build_role,
        account_query_role=args.query_role,
        account_warehouse=args.warehouse,
        account_database=args.database,
        account_schema=args.schema,
    )

    if len(args.commands) == 0:
        raise ValueError("No command is provided")

    if "build_view" in args.commands:
        build_view(
            conn=conn,
            stats_sample_method=args.sample_method,
        )
    if "load_lineage" in args.commands:
        qdc_client = qdc.QDCExternalAPIClient(
            base_url=args.api_url,
            client_id=args.client_id,
            client_secret=args.client_secret,
        )
        load_lineage(
            conn=conn,
            qdc_client=qdc_client,
            tenant_id=args.tenant_id,
        )
    if "load_stats" in args.commands:
        qdc_client = qdc.QDCExternalAPIClient(
            base_url=args.api_url,
            client_id=args.client_id,
            client_secret=args.client_secret,
        )
        load_stats(
            conn=conn,
            qdc_client=qdc_client,
            tenant_id=args.tenant_id,
        )