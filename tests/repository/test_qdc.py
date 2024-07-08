import os
import sys
import unittest
from unittest.mock import patch

import responses

sys.path.insert(0, os.path.abspath("../.."))

from quollio_core.repository.qdc import QDCExternalAPIClient, initialize_qdc_client


class TestQDCExternalAPIClient(unittest.TestCase):
    @patch("quollio_core.repository.qdc.QDCExternalAPIClient._get_auth_token", return_value="fake_token")
    def setUp(self, mock_get_auth_token):
        self.client = QDCExternalAPIClient(
            base_url="http://testqdc.com", client_id="fake_client_id", client_secret="fake_client_secret"
        )

    @patch("jwt.decode", return_value={"exp": 1720079532})  # Thu Jul 04 2024 07:52:12 GMT+0000
    @patch("quollio_core.repository.qdc.QDCExternalAPIClient._get_auth_token", return_value="after_refreshed_token")
    @patch("time.time", side_effect=[1720079531, 1720079532, 1720079533])
    def test_refresh_token_if_expired(self, mock_jwt_decode, mock_get_auth_token, mock_time):
        self.client._refresh_token_if_expired()  # current timestamp < expired timestamp
        self.assertEqual(self.client.auth_token, "fake_token")

        self.client._refresh_token_if_expired()  # current timestamp = expired timestamp
        self.assertEqual(self.client.auth_token, "fake_token")

        self.client._refresh_token_if_expired()  # current timestamp > expired timestamp
        self.assertEqual(self.client.auth_token, "after_refreshed_token")

    @responses.activate
    @patch("quollio_core.repository.qdc.QDCExternalAPIClient._refresh_token_if_expired", return_value=None)
    def test_update_stats_by_id_retries(self, mock_refresh_token_if_expired):
        global_id = "tbl-12345"
        payload = {"tbl-12345": ["tbl-67890"]}

        test_cases = [
            {
                "input": {
                    "code": 200,
                },
                "expect": {
                    "execution_count": 1,
                },
            },
            {
                "input": {
                    "code": 400,
                },
                "expect": {
                    "execution_count": 1,
                },
            },
            {
                "input": {
                    "code": 401,
                },
                "expect": {
                    "execution_count": 1,
                },
            },
            {
                "input": {
                    "code": 403,
                },
                "expect": {
                    "execution_count": 1,
                },
            },
            {
                "input": {
                    "code": 404,
                },
                "expect": {
                    "execution_count": 1,
                },
            },
            {
                "input": {
                    "code": 429,
                },
                "expect": {
                    "execution_count": 10,
                },
            },
            {
                "input": {
                    "code": 500,
                },
                "expect": {
                    "execution_count": 10,
                },
            },
            {
                "input": {
                    "code": 503,
                },
                "expect": {
                    "execution_count": 10,
                },
            },
            {
                "input": {
                    "code": 504,
                },
                "expect": {
                    "execution_count": 10,
                },
            },
        ]
        for test_case in test_cases:
            responses.add(
                responses.PUT,
                "{base_url}/v2/assets/{global_id}/stats".format(base_url=self.client.base_url, global_id=global_id),
                json={"error": "error test"},
                status=test_case["input"]["code"],
            )
            self.client.update_stats_by_id(global_id, payload)
            self.assertEqual(test_case["expect"]["execution_count"], len(responses.calls))
            responses.reset()

    @responses.activate
    @patch("quollio_core.repository.qdc.QDCExternalAPIClient._refresh_token_if_expired", return_value=None)
    def test_update_lineage_by_id_retries(self, mock_refresh_token_if_expired):
        global_id = "tbl-12345"
        payload = {"tbl-12345": ["tbl-67890"]}

        test_cases = [
            {
                "input": {
                    "code": 200,
                },
                "expect": {
                    "execution_count": 1,
                },
            },
            {
                "input": {
                    "code": 400,
                },
                "expect": {
                    "execution_count": 1,
                },
            },
            {
                "input": {
                    "code": 401,
                },
                "expect": {
                    "execution_count": 1,
                },
            },
            {
                "input": {
                    "code": 403,
                },
                "expect": {
                    "execution_count": 1,
                },
            },
            {
                "input": {
                    "code": 404,
                },
                "expect": {
                    "execution_count": 1,
                },
            },
            {
                "input": {
                    "code": 429,
                },
                "expect": {
                    "execution_count": 10,
                },
            },
            {
                "input": {
                    "code": 500,
                },
                "expect": {
                    "execution_count": 10,
                },
            },
            {
                "input": {
                    "code": 503,
                },
                "expect": {
                    "execution_count": 10,
                },
            },
            {
                "input": {
                    "code": 504,
                },
                "expect": {
                    "execution_count": 10,
                },
            },
        ]
        for test_case in test_cases:
            responses.add(
                responses.PUT,
                "{base_url}/v2/lineage/{global_id}".format(base_url=self.client.base_url, global_id=global_id),
                json={"error": "error test"},
                status=test_case["input"]["code"],
            )
            self.client.update_lineage_by_id(global_id, payload)
            self.assertEqual(test_case["expect"]["execution_count"], len(responses.calls))
            responses.reset()

    @patch("quollio_core.repository.qdc.QDCExternalAPIClient._get_auth_token", return_value="fake_token")
    def test_initialize_qdc_client(self, mock_get_auth_token):
        res = initialize_qdc_client(
            api_url="http://testqdc.com", client_id="fake_client_id", client_secret="fake_client_secret"
        )
        self.assertEqual(type(res), QDCExternalAPIClient)


if __name__ == "__main__":
    unittest.main()
