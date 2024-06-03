import logging

from google.cloud.bigquery import Client
from google.cloud.datacatalog_lineage_v1 import EntityReference, LineageClient, SearchLinksRequest
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)


class BigQueryClient:
    def __init__(self, credentials: Credentials) -> None:
        self.client = self.__initialze(credentials=credentials)

    def __initialze(self, credentials: Credentials) -> Client:
        client = Client(credentials=credentials)
        return client

    def list_datasets(self, project_id) -> list:
        datasets = list(self.client.list_datasets(project_id))
        logger.debug("Found %s datasets in project %s", len(datasets), project_id)
        return datasets

    def list_tables(self, dataset_id) -> list:
        tables = list(self.client.list_tables(dataset_id))
        logger.debug("Found %s tables in dataset %s", len(tables), dataset_id)
        return list(self.client.list_tables(dataset_id))


class GCPLineageClient:
    def __init__(self, credentials: Credentials) -> None:
        self.client = self.__initialze(credentials=credentials)

    def __initialze(self, credentials: Credentials) -> LineageClient:
        client = LineageClient(credentials=credentials)
        return client

    def get_links(self, request: SearchLinksRequest) -> list:
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
    org_id = project["parent"]["id"]
    return org_id
