import argparse
import logging
import os

from quollio_core.helper.core import setup_dbt_profile, trim_prefix
from quollio_core.helper.env_default import env_default
from quollio_core.profilers.databricks import (
    databricks_column_level_lineage,
    databricks_column_stats,
    databricks_table_level_lineage,
)
from quollio_core.repository import databricks as db
from quollio_core.repository import dbt, qdc

logger = logging.getLogger(__name__)


def build_view(
    conn: db.DatabricksConnectionConfig,
    target_tables: str = "",
    log_level: str = "info",
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    logger.info("Build profiler views using dbt")
    # set parameters
    dbt_client = dbt.DBTClient()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_path = f"{current_dir}/dbt_projects/databricks"
    template_path = f"{current_dir}/dbt_projects/databricks/profiles"
    template_name = "profiles_template.yml"

    # build views using dbt
    setup_dbt_profile(connections_json=conn.as_dict(), template_path=template_path, template_name=template_name)
    # FIXME: when executing some of the commands, directory changes due to the library bug.
    # https://github.com/dbt-labs/dbt-core/issues/8997
    dbt_client.invoke(
        cmd="deps",
        project_dir=project_path,
        profile_dir=template_path,
        options=["--no-use-colors", "--log-level", log_level],
    )

    run_options = ["--no-use-colors", "--log-level", log_level]

    if target_tables is not None:
        target_tables_str = " ".join(target_tables)
        run_options.append("--select")
        run_options.append(target_tables_str)

    dbt_client.invoke(
        cmd="run",
        project_dir=project_path,
        profile_dir=template_path,
        options=run_options,
    )
    return


def load_lineage(
    conn: db.DatabricksConnectionConfig,
    endpoint: str,
    qdc_client: qdc.QDCExternalAPIClient,
    tenant_id: str,
    enable_column_lineage: bool = False,
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    logger.info("Generate Databricks table to table lineage.")
    databricks_table_level_lineage(
        conn=conn,
        endpoint=endpoint,
        qdc_client=qdc_client,
        tenant_id=tenant_id,
        dbt_table_name="quollio_lineage_table_level",
    )

    if enable_column_lineage:
        logger.info(
            f"enable_column_lineage is set to {enable_column_lineage}.Generate Databricks column to column lineage."
        )
        databricks_column_level_lineage(
            conn=conn,
            endpoint=endpoint,
            qdc_client=qdc_client,
            tenant_id=tenant_id,
            dbt_table_name="quollio_lineage_column_level",
        )
    else:
        logger.info("Skip column lineage ingestion. Set enable_column_lineage to True if you ingest column lineage.")

    logger.info("Lineage data is successfully loaded.")
    return


def load_column_stats(
    conn: db.DatabricksConnectionConfig,
    endpoint: str,
    qdc_client: qdc.QDCExternalAPIClient,
    tenant_id: str,
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    logger.info("Generate Databricks column stats.")
    databricks_column_stats(
        conn=conn,
        endpoint=endpoint,
        qdc_client=qdc_client,
        tenant_id=tenant_id,
    )

    logger.info("Column stats are successfully loaded.")
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Quollio Intelligence Agent for Databricks",
        description="Build views and load lineage and stats to Quollio from Databricks using dbt.",
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
        'load_stats': Load stats from created views to Quollio,
        """,
    )
    parser.add_argument(
        "--host", type=str, action=env_default("DATABRICKS_HOST"), required=False, help="Host for Databricks workspace"
    )
    parser.add_argument(
        "--http_path",
        type=str,
        action=env_default("DATABRICKS_HTTP_PATH"),
        required=False,
        help="HTTP path for a Databricks compute resource (i.e warehouse)",
    )
    parser.add_argument(
        "--port",
        type=int,
        action=env_default("DATABRICKS_PORT"),
        required=False,
        help="Port for Databricks compute resource",
    )
    parser.add_argument(
        "--databricks_client_secret",
        type=str,
        action=env_default("DATABRICKS_CLIENT_SECRET"),
        required=False,
        help="Secret for the service principal",
    )
    parser.add_argument(
        "--databricks_client_id",
        type=str,
        action=env_default("DATABRICKS_CLIENT_ID"),
        required=False,
        help="Client id for the service principal",
    )
    parser.add_argument(
        "--catalog",
        type=str,
        required=False,
        action=env_default("DATABRICKS_TARGET_CATALOG"),
        help="Target database name where the views are built by dbt",
    )
    parser.add_argument(
        "--schema",
        type=str,
        action=env_default("DATABRICKS_TARGET_SCHEMA"),
        required=False,
        help="Target schema name where the views are built by dbt",
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
    parser.add_argument(
        "--tenant_id",
        type=str,
        action=env_default("TENANT_ID"),
        required=False,
        help="The tenant id (company id) where the lineage and stats are loaded",
    )
    parser.add_argument(
        "--target_tables",
        type=str,
        nargs="+",
        choices=["quollio_lineage_table_level", "quollio_lineage_column_level"],
        action=env_default("DATABRICKS_TARGET_TABLES"),
        required=False,
        help="Target tables you want to create with dbt module. \
              You need to specify this parameter if you want to specify tables, not all ones. \
              Please specify table name with blank delimiter like tableA tableB \
              if you want to create two or more tables",
    )
    parser.add_argument(
        "--monitoring_table_suffix",
        type=str,
        action=env_default("DATABRICKS_MONITORING_TABLE_SUFFIX"),
        required=False,
        help="Sets the monitoring tables suffix for databricks. \
              This is used to identify the monitoring tables created by the databricks monitoring tool. \
              Default value is _profile_metrics",
    )
    parser.add_argument(
        "--enable_column_lineage",
        type=bool,
        action=env_default("ENABLE_COLUMN_LINEAGE", store_true=True),
        default=False,
        required=False,
        help="Whether to ingest column lineage into QDIC or not. Default value is False",
    )

    args = parser.parse_args()

    conn = db.DatabricksConnectionConfig(
        # MEMO: Metadata agent allows the string 'https://' as a host name but is not allowed by intelligence agent.
        host=trim_prefix(args.host, "https://"),
        http_path=args.http_path,
        client_id=args.databricks_client_id,
        client_secret=args.databricks_client_secret,
        catalog=args.catalog,
        schema=args.schema,
    )

    if len(args.commands) == 0:
        raise ValueError("No command is provided")

    if "build_view" in args.commands:
        build_view(
            conn=conn,
            target_tables=args.target_tables,
            log_level=args.log_level,
        )

    if "load_lineage" in args.commands:
        qdc_client = qdc.QDCExternalAPIClient(
            base_url=args.api_url, client_id=args.client_id, client_secret=args.client_secret
        )
        load_lineage(
            conn=conn,
            endpoint=args.host,
            qdc_client=qdc_client,
            tenant_id=args.tenant_id,
            enable_column_lineage=args.enable_column_lineage,
        )

    if "load_stats" in args.commands:
        qdc_client = qdc.QDCExternalAPIClient(
            base_url=args.api_url, client_id=args.client_id, client_secret=args.client_secret
        )
        databricks_column_stats(
            conn=conn,
            endpoint=args.host,
            qdc_client=qdc_client,
            tenant_id=args.tenant_id,
            monitoring_table_suffix=args.monitoring_table_suffix,
        )
