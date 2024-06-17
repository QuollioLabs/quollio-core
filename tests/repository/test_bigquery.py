import json
import unittest
from unittest.mock import MagicMock, patch

from google.oauth2.service_account import Credentials

from quollio_core.repository.bigquery import (
    EntityReference,
    get_credentials,
    get_entitiy_reference,
    get_org_id,
    get_search_request,
)


class TestBigQueryClients(unittest.TestCase):
    def test_get_entitiy_reference(self):
        # Test if get_entitiy_reference returns an EntityReference instance
        result = get_entitiy_reference()
        self.assertIsInstance(result, EntityReference)

    def test_get_search_request(self):
        # Mocking EntityReference
        downstream_table = get_entitiy_reference()
        downstream_table.fully_qualified_name = "bigquery:qdc123.test_dataset.test_table"
        project_id = "test_project"
        region = "asia-northeast1"

        # Expected request
        expected_request = get_search_request(downstream_table=downstream_table, project_id=project_id, region=region)

        # Test if get_search_request returns the correct SearchLinksRequest
        result = get_search_request(downstream_table, project_id, region)
        self.assertEqual(result.target, expected_request.target)
        self.assertEqual(result.parent, expected_request.parent)

    @patch("quollio_core.repository.bigquery.Credentials.from_service_account_info")
    def test_get_credentials(self, mock_from_service_account_info):
        credential_json = """
        {
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
            "universe_domain": "googleapis.com"
        }
        """

        credentials_dict = json.loads(credential_json)

        # Mock the return value of from_service_account_info
        mock_credentials = MagicMock(spec=Credentials)
        mock_from_service_account_info.return_value = mock_credentials

        # Call the function
        credential = get_credentials(credentials_dict)

        # Verify the function calls
        mock_from_service_account_info.assert_called_once_with(credentials_dict)
        self.assertIsInstance(credential, Credentials)

    @patch("quollio_core.repository.bigquery.build")
    @patch("quollio_core.repository.bigquery.get_credentials")
    def test_get_org_id(self, mock_get_credentials, mock_build):
        # Mocking the Google API client build function
        mock_service = MagicMock()
        mock_projects = mock_service.projects.return_value
        mock_projects.get.return_value.execute.return_value = {"parent": {"id": "1234567890"}}
        mock_build.return_value = mock_service

        # Mock the get_credentials function
        mock_credentials = MagicMock(spec=Credentials)
        mock_get_credentials.return_value = mock_credentials

        # Create a mock credentials JSON
        credential_json = """
        {
            "type": "service_account",
            "project_id": "test_project",
            "private_key_id": "asfasfafasf",
            "private_key": "-----BEGIN PRIVATE KEY-----\\nlklklkl----END PRIVATE KEY-----\\n",
            "client_email": "testt@test.iam.gserviceaccount.com",
            "client_id": "33333",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/asda/x509/a@da.iam.gserviceaccount.com",
            "universe_domain": "googleapis.com"
        }
        """

        credentials_dict = json.loads(credential_json)
        # Call the function with the mocked service
        org_id = get_org_id(credentials_dict)

        # Assertions
        mock_get_credentials.assert_called_once_with(credentials_dict)
        mock_build.assert_called_once_with("cloudresourcemanager", "v1", credentials=mock_credentials)
        mock_projects.get.assert_called_once_with(projectId="test_project")
        self.assertEqual(org_id, "1234567890")


if __name__ == "__main__":
    unittest.main()
