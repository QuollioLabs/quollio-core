import argparse
import json
import logging

from quollio_core.helper.env_default import env_default
from quollio_core.profilers.bigquery import bigquery_table_lineage
from quollio_core.repository import qdc
from quollio_core.repository.bigquery import get_credentials, get_org_id

logger = logging.getLogger(__name__)


def load_lineage(
    qdc_client: qdc.QDCExternalAPIClient, project_id: str, regions: list, tenant_id: str, credentials: dict, org_id: str
):
    bigquery_table_lineage(
        qdc_client=qdc_client,
        tenant_id=tenant_id,
        project_id=project_id,
        regions=regions,
        credentials=credentials,
        org_id=org_id,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Quollio Intelligence Agent for Google BigQuery",
        description="Collect lineage and stats from Google BigQuery and load to Quollio Data Catalog",
        epilog="Copyright (c) 2024 Quollio Technologies, Inc.",
    )
    parser.add_argument(
        "commands",
        choices=["load_lineage"],
        type=str,
        nargs="+",
        help="""
        The command to execute.
        'load_lineage': Load lineage data from Google Data Catalog to Quollio,
        """,
    )
    parser.add_argument(
        "--credentials",
        type=str,
        action=env_default("GOOGLE_APPLICATION_CREDENTIALS"),
        help="Crendentials for Google Cloud Platform",
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
        help="The client secret that is created on Quollio console to let clients access Quollio External API",
    )
    parser.add_argument(
        "--project_id",
        type=str,
        action=env_default("GCP_PROJECT_ID"),
        required=False,
        help="GCP Project ID",
    )
    parser.add_argument(
        "--regions",
        type=str,
        action=env_default("GCP_REGIONS"),
        required=False,
        help="GCP regions where the data is located. Multiple regions can be provided separated by space.",
        nargs="+",
    )

    args = parser.parse_args()

    if len(args.commands) == 0:
        raise ValueError("No command is provided")

    if "load_lineage" in args.commands:

        qdc_client = qdc.QDCExternalAPIClient(
            base_url=args.api_url, client_id=args.client_id, client_secret=args.client_secret
        )

        credentials_json = json.loads(args.credentials)
        credentials = get_credentials(credentials_json=credentials_json)
        org_id = get_org_id(credentials_json=credentials_json)

        load_lineage(
            qdc_client=qdc_client,
            project_id=args.project_id,
            regions=args.regions,
            tenant_id=args.tenant_id,
            credentials=credentials,
            org_id=org_id,
        )
