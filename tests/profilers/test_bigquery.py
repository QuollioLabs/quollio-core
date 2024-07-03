import unittest
from unittest.mock import Mock, patch

from google.auth.credentials import Credentials

from quollio_core.profilers.bigquery import (
    bigquery_table_lineage,
    bigquery_table_stats,
    column_stats_from_dataplex,
    generate_lineage_links,
    generate_table_list,
)


class TestBigQueryProfilers(unittest.TestCase):

    @patch("quollio_core.profilers.bigquery.qdc.QDCExternalAPIClient")
    @patch("quollio_core.profilers.bigquery.BigQueryClient")
    @patch("quollio_core.profilers.bigquery.GCPLineageClient")
    def test_bigquery_table_lineage(self, MockGCPLineageClient, MockBigQueryClient, MockQDCExternalAPIClient):
        mock_credentials = Mock(spec=Credentials)
        mock_bq_client = MockBigQueryClient.return_value
        mock_lineage_client = MockGCPLineageClient.return_value
        mock_qdc_client = MockQDCExternalAPIClient.return_value
        mock_bq_client.list_dataset_ids.return_value = ["dataset1"]
        mock_bq_client.list_tables.return_value = [
            {"table_id": "table1", "table_type": "TABLE", "dataset_id": "dataset1"}
        ]
        mock_lineage_client.get_links.return_value = []

        bigquery_table_lineage(
            qdc_client=mock_qdc_client,
            tenant_id="tenant_id",
            project_id="project_id",
            regions=["region1"],
            org_id="org_id",
            credentials=mock_credentials,
        )

        mock_bq_client.list_dataset_ids.assert_called_once()
        mock_bq_client.list_tables.assert_called_once_with("dataset1")
        mock_lineage_client.get_links.assert_called_once()

    @patch("quollio_core.profilers.bigquery.qdc.QDCExternalAPIClient")
    @patch("quollio_core.profilers.bigquery.BigQueryClient")
    def test_bigquery_table_stats(self, MockBigQueryClient, MockQDCExternalAPIClient):
        mock_bq_client = MockBigQueryClient.return_value
        mock_qdc_client = MockQDCExternalAPIClient.return_value
        mock_bq_client.client.query.return_value.result.return_value = [
            {
                "DB_NAME": "db",
                "SCHEMA_NAME": "schema",
                "TABLE_NAME": "table",
                "COLUMN_NAME": "column",
                "MIN_VALUE": 1,
                "MAX_VALUE": 100,
                "AVG_VALUE": 50,
                "MEDIAN_VALUE": 50,
                "STDDEV_VALUE": 10,
                "MODE_VALUE": 50,
                "NULL_COUNT": 0,
                "CARDINALITY": 100,
            }
        ]

        bigquery_table_stats(
            qdc_client=mock_qdc_client,
            bq_client=mock_bq_client,
            tenant_id="tenant_id",
            org_id="org_id",
            dataplex_stats_tables=["dataplex_stats_table"],
        )

        mock_bq_client.client.query.assert_called_once()
        mock_qdc_client.update_stats_by_id.assert_called_once()

    @patch("quollio_core.profilers.bigquery.BigQueryClient")
    def test_generate_table_list(self, MockBigQueryClient):
        mock_bq_client = MockBigQueryClient.return_value
        mock_bq_client.list_tables.return_value = [
            {"table_id": "table1", "table_type": "TABLE", "dataset_id": "dataset1"},
            {"table_id": "view1", "table_type": "VIEW", "dataset_id": "dataset1"},
        ]
        mock_bq_client.client.project = "test-project"

        table_list = generate_table_list(mock_bq_client, ["dataset1"])

        expected_table_list = ["test-project.dataset1.table1", "test-project.dataset1.view1"]
        self.assertEqual(table_list, expected_table_list)
        mock_bq_client.list_tables.assert_called_once_with("dataset1")

    @patch("quollio_core.profilers.bigquery.GCPLineageClient")
    @patch("quollio_core.profilers.bigquery.get_entitiy_reference")
    @patch("quollio_core.profilers.bigquery.get_search_request")
    def test_generate_lineage_links(self, MockGetSearchRequest, MockGetEntityReference, MockGCPLineageClient):
        mock_lineage_client = MockGCPLineageClient.return_value
        mock_entity_reference = MockGetEntityReference.return_value
        mock_entity_reference.fully_qualified_name = "bigquery:test-project.dataset1.table1"
        mock_search_request = MockGetSearchRequest.return_value
        mock_lineage_client.get_links.return_value = []

        lineage_links = generate_lineage_links(
            all_tables=["test-project.dataset1.table1"],
            lineage_client=mock_lineage_client,
            project_id="test-project",
            regions=["region1"],
        )

        self.assertEqual(lineage_links, {})
        MockGetEntityReference.assert_called_once()
        MockGetSearchRequest.assert_called_once_with(
            downstream_table=mock_entity_reference, project_id="test-project", region="region1"
        )
        mock_lineage_client.get_links.assert_called_once_with(request=mock_search_request)

    @patch("quollio_core.profilers.bigquery.logger")
    @patch("quollio_core.profilers.bigquery.BigQueryClient")
    def test_column_stats_from_dataplex(self, MockBigQueryClient, mock_logger):
        mock_bq_client = MockBigQueryClient.return_value
        mock_bq_client.client.query.return_value.result.return_value = [
            {
                "DB_NAME": "db",
                "SCHEMA_NAME": "schema",
                "TABLE_NAME": "table",
                "COLUMN_NAME": "column",
                "MIN_VALUE": 1,
                "MAX_VALUE": 100,
                "AVG_VALUE": 50,
                "MEDIAN_VALUE": 50,
                "STDDEV_VALUE": 10,
                "MODE_VALUE": 50,
                "NULL_COUNT": 0,
                "CARDINALITY": 100,
            }
        ]

        results = column_stats_from_dataplex(mock_bq_client, "dataplex_stats_table")

        expected_results = [
            {
                "DB_NAME": "db",
                "SCHEMA_NAME": "schema",
                "TABLE_NAME": "table",
                "COLUMN_NAME": "column",
                "MIN_VALUE": 1,
                "MAX_VALUE": 100,
                "AVG_VALUE": 50,
                "MEDIAN_VALUE": 50,
                "STDDEV_VALUE": 10,
                "MODE_VALUE": 50,
                "NULL_COUNT": 0,
                "CARDINALITY": 100,
            }
        ]

        self.assertEqual(results, expected_results)
        mock_bq_client.client.query.assert_called_once()
        mock_logger.debug.assert_called_once_with(
            "Executing Query: \n"
            "    SELECT\n"
            "        data_source.table_project_id AS DB_NAME,\n"
            "        data_source.dataset_id AS SCHEMA_NAME,\n"
            "        data_source.table_id AS TABLE_NAME,\n"
            "        column_name AS COLUMN_NAME,\n"
            "        min_value AS MIN_VALUE,\n"
            "        max_value AS MAX_VALUE,\n"
            "        average_value AS AVG_VALUE,\n"
            "        quartile_median AS MEDIAN_VALUE,\n"
            "        standard_deviation AS STDDEV_VALUE,\n"
            "        top_n[0][0] AS MODE_VALUE,\n"
            "        CAST((percent_null / 100) * job_rows_scanned AS INT) as NULL_COUNT,\n"
            "        CAST((percent_unique / 100) * job_rows_scanned AS INT) as CARDINALITY\n"
            "    FROM `dataplex_stats_table`\n    "
        )


if __name__ == "__main__":
    unittest.main()
