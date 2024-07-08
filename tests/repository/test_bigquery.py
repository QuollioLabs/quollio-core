import unittest
from unittest.mock import Mock, patch

from google.cloud.datacatalog_lineage_v1 import SearchLinksRequest
from google.oauth2.service_account import Credentials

from quollio_core.repository.bigquery import BigQueryClient, GCPLineageClient, get_credentials, get_org_id


class TestBigQueryClient(unittest.TestCase):
    @patch("quollio_core.repository.bigquery.Client")
    def setUp(self, MockClient):
        self.credentials = Mock(spec=Credentials)
        self.project_id = "test-project"
        self.mock_bq_client = MockClient.return_value
        self.mock_bq_client.project = self.project_id
        self.bq_client = BigQueryClient(credentials=self.credentials, project_id=self.project_id)

    def test_list_dataset_ids(self):
        mock_dataset = Mock()
        mock_dataset.dataset_id = "test_dataset"
        self.mock_bq_client.list_datasets.return_value = [mock_dataset]

        dataset_ids = self.bq_client.list_dataset_ids()

        self.assertEqual(dataset_ids, ["test_dataset"])
        self.mock_bq_client.list_datasets.assert_called_once()

    def test_list_tables(self):
        mock_table = Mock()
        mock_table.table_id = "test_table"
        mock_table.table_type = "TABLE"
        mock_table.project = self.project_id
        mock_table.dataset_id = "test_dataset"
        self.mock_bq_client.list_tables.return_value = [mock_table]

        tables = self.bq_client.list_tables("test_dataset")

        expected_result = [
            {"table_id": "test_table", "table_type": "TABLE", "project": self.project_id, "dataset_id": "test_dataset"}
        ]
        self.assertEqual(tables, expected_result)
        self.mock_bq_client.list_tables.assert_called_once_with("test_dataset")

    def test_get_columns(self):
        mock_field = Mock()
        mock_field.name = "column1"
        mock_field.field_type = "STRING"
        mock_table = Mock()
        mock_table.schema = [mock_field]
        self.mock_bq_client.get_table.return_value = mock_table

        columns = self.bq_client.get_columns("test_table", "test_dataset")

        expected_result = [{"name": "column1", "type": "STRING"}]
        self.assertEqual(columns, expected_result)
        self.mock_bq_client.get_table.assert_called_once_with(f"{self.project_id}.test_dataset.test_table")

    def test_get_all_columns(self):
        self.bq_client.list_dataset_ids = Mock(return_value=["test_dataset"])
        self.bq_client.list_tables = Mock(return_value=[{"table_id": "test_table", "table_type": "TABLE"}])
        self.bq_client.get_columns = Mock(return_value=[{"name": "column1", "type": "STRING"}])

        all_columns = self.bq_client.get_all_columns()

        expected_result = {
            "test_dataset": {"test_table": {"columns": [{"name": "column1", "type": "STRING"}], "table_type": "TABLE"}}
        }
        self.assertEqual(all_columns, expected_result)
        self.bq_client.list_dataset_ids.assert_called_once()
        self.bq_client.list_tables.assert_called_once_with("test_dataset")
        self.bq_client.get_columns.assert_called_once_with("test_table", "test_dataset")


class TestGCPLineageClient(unittest.TestCase):
    @patch("quollio_core.repository.bigquery.LineageClient")
    def setUp(self, MockLineageClient):
        self.credentials = Mock(spec=Credentials)
        self.mock_lineage_client = MockLineageClient.return_value
        self.lineage_client = GCPLineageClient(credentials=self.credentials)

    def test_get_links(self):
        mock_response = Mock()
        mock_response.links = ["link1", "link2"]
        self.mock_lineage_client.search_links.return_value = mock_response
        request = Mock(spec=SearchLinksRequest)

        links = self.lineage_client.get_links(request)

        self.assertEqual(links, ["link1", "link2"])
        self.mock_lineage_client.search_links.assert_called_once_with(request)


class TestUtilityFunctions(unittest.TestCase):
    @patch("quollio_core.repository.bigquery.Credentials")
    def test_get_credentials(self, MockCredentials):
        mock_credentials_json = {
            "type": "service_account",
            "project_id": "asfasfafas",
            "private_key_id": "asfasfafasf",
            "private_key": "-----BEGIN PRIVATE KEY-----\\nlklklkl----END PRIVATE KEY-----\\n",
            "client_email": "testt@test.iam.gserviceaccount.com",
            "client_id": "33333",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/asda/x509/a@da.iam.gserviceaccount.com",
            "universe_domain": "googleapis.com",
        }
        MockCredentials.from_service_account_info.return_value = "mocked_credentials"

        credentials = get_credentials(mock_credentials_json)

        self.assertEqual(credentials, "mocked_credentials")
        MockCredentials.from_service_account_info.assert_called_once_with(mock_credentials_json)

    @patch("quollio_core.repository.bigquery.build")
    @patch("quollio_core.repository.bigquery.get_credentials")
    def test_get_org_id(self, mock_get_credentials, mock_build):
        mock_credentials = Mock(spec=Credentials)
        mock_get_credentials.return_value = mock_credentials
        mock_crm_service = Mock()
        mock_build.return_value = mock_crm_service
        mock_crm_service.projects.return_value.get.return_value.execute.return_value = {"parent": {"id": "test_org_id"}}
        mock_credentials_json = {"project_id": "test_project"}

        org_id = get_org_id(mock_credentials_json)

        self.assertEqual(org_id, "test_org_id")
        mock_get_credentials.assert_called_once_with(mock_credentials_json)
        mock_build.assert_called_once_with("cloudresourcemanager", "v1", credentials=mock_credentials)
        mock_crm_service.projects.return_value.get.return_value.execute.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
