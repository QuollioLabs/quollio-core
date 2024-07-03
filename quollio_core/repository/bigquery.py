from typing import Any, Dict, List

from google.cloud.bigquery import Client
from google.cloud.datacatalog_lineage_v1 import EntityReference, LineageClient, SearchLinksRequest
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from quollio_core.helper.log_utils import logger  # Importing the logger from logging_utils


class BigQueryClient:
    """Client to interact with the BigQuery API."""

    def __init__(self, credentials: Credentials, project_id: str) -> None:
        """Initialize the BigQuery client with provided credentials."""
        self.client = self.__initialize(credentials=credentials, project_id=project_id)

    def __initialize(self, credentials: Credentials, project_id: str) -> Client:
        return Client(credentials=credentials, project=project_id)

    def list_dataset_ids(self) -> List[str]:
        """List all dataset ids in the project."""
        datasets = list(self.client.list_datasets())
        logger.debug("Found %s datasets in project %s", len(datasets), self.client.project)
        return [dataset.dataset_id for dataset in datasets]

    def list_tables(self, dataset_id: str) -> List[Dict[str, str]]:
        """List all tables in the dataset."""
        tables = list(self.client.list_tables(dataset_id))
        logger.debug("Found %s tables in dataset %s", len(tables), dataset_id)
        return [
            {
                "table_id": table.table_id,
                "table_type": table.table_type,
                "project": table.project,
                "dataset_id": table.dataset_id,
            }
            for table in tables
        ]

    def get_columns(self, table_id: str, dataset_id: str) -> List[Dict[str, str]]:
        """Get the columns of the table."""
        table = self.client.get_table(f"{self.client.project}.{dataset_id}.{table_id}")
        return [{"name": field.name, "type": field.field_type} for field in table.schema]

    def get_all_columns(self) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        """Get all columns in the project."""
        all_columns = {}
        datasets = self.list_dataset_ids()
        for dataset_id in datasets:
            all_columns[dataset_id] = {}
            tables = self.list_tables(dataset_id)
            for table_info in tables:
                table_id = table_info["table_id"]
                table_type = table_info["table_type"]
                columns = self.get_columns(table_id, dataset_id)
                all_columns[dataset_id][table_id] = {"columns": columns, "table_type": table_type}
        return all_columns


class GCPLineageClient:
    """Client to interact with the GCP Lineage API."""

    def __init__(self, credentials: Credentials) -> None:
        """Initialize the GCP Lineage client with provided credentials."""
        self.client = self.__initialze(credentials=credentials)

    def __initialze(self, credentials: Credentials) -> LineageClient:
        return LineageClient(credentials=credentials)

    def get_links(self, request: SearchLinksRequest) -> list:
        """Search for links between entities (tables)."""
        response = self.client.search_links(request)
        return response.links


def get_entitiy_reference() -> EntityReference:
    return EntityReference()


def get_search_request(downstream_table: EntityReference, project_id: str, region: str) -> SearchLinksRequest:
    return SearchLinksRequest(target=downstream_table, parent=f"projects/{project_id}/locations/{region.lower()}")


def get_credentials(credentials_json: dict) -> Credentials:
    return Credentials.from_service_account_info(credentials_json)


def get_org_id(credentials_json: dict) -> str:
    credentials = get_credentials(credentials_json)
    crm_service = build("cloudresourcemanager", "v1", credentials=credentials)
    project_id = credentials_json["project_id"]
    project = crm_service.projects().get(projectId=project_id).execute()
    return project["parent"]["id"]
