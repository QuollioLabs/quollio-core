import unittest
from unittest.mock import MagicMock, patch

from quollio_core.profilers.bigquery import generate_lineage_links, generate_table_list


class TestGenerateTableList(unittest.TestCase):
    def setUp(self):
        self.bq_client = MagicMock()

        self.dataset_1 = MagicMock()
        self.dataset_1.dataset_id = "dataset_1"

        self.dataset_2 = MagicMock()
        self.dataset_2.dataset_id = "dataset_2"

        self.datasets = [self.dataset_1, self.dataset_2]

        # Mock tables
        self.table_1 = MagicMock()
        self.table_1.project = "project_1"
        self.table_1.dataset_id = "dataset_1"
        self.table_1.table_id = "table_1"
        self.table_1.table_type = "TABLE"

        self.table_2 = MagicMock()
        self.table_2.project = "project_1"
        self.table_2.dataset_id = "dataset_2"
        self.table_2.table_id = "table_2"
        self.table_2.table_type = "VIEW"

        self.table_3 = MagicMock()
        self.table_3.project = "project_1"
        self.table_3.dataset_id = "dataset_1"
        self.table_3.table_id = "table_3"
        self.table_3.table_type = "MATERIALIZED_VIEW"

        self.table_4 = MagicMock()
        self.table_4.project = "project_1"
        self.table_4.dataset_id = "dataset_2"
        self.table_4.table_id = "table_4"
        self.table_4.table_type = "EXTERNAL"

    def test_generate_table_list(self):
        # Mock the list_tables method
        self.bq_client.list_tables.side_effect = [
            [self.table_1, self.table_3],  # Tables in dataset_1
            [self.table_2, self.table_4],  # Tables in dataset_2
        ]

        expected_table_names = [
            "project_1.dataset_1.table_1",
            "project_1.dataset_1.table_3",
            "project_1.dataset_2.table_2",
        ]

        result = generate_table_list(self.datasets, self.bq_client)
        self.assertEqual(result, expected_table_names)


class TestGenerateLineageLinks(unittest.TestCase):
    @patch("quollio_core.repository.bigquery.get_entitiy_reference")
    @patch("quollio_core.repository.bigquery.get_search_request")
    def test_generate_lineage_links(self, mock_get_search_request, mock_get_entitiy_reference):
        # Setup
        lineage_client = MagicMock()
        project_id = "test_project"
        regions = ["us-central1", "europe-west1"]
        all_tables = ["project_1.dataset_1.table_1", "project_1.dataset_2.table_2"]

        # Mock downstream table entity reference
        downstream = MagicMock()
        mock_get_entitiy_reference.return_value = downstream

        # Mock search request and response
        request = MagicMock()
        mock_get_search_request.return_value = request

        lineage_response_1 = [
            MagicMock(
                source=MagicMock(fully_qualified_name="bigquery:source_1"),
                target=MagicMock(fully_qualified_name="bigquery:target_1"),
            ),
            MagicMock(
                source=MagicMock(fully_qualified_name="bigquery:source_2"),
                target=MagicMock(fully_qualified_name="bigquery:target_1"),
            ),
        ]
        lineage_response_2 = [
            MagicMock(
                source=MagicMock(fully_qualified_name="bigquery:source_3"),
                target=MagicMock(fully_qualified_name="bigquery:target_2"),
            )
        ]

        # Mock lineage client response
        lineage_client.get_links.side_effect = [
            lineage_response_1,
            lineage_response_2,
            lineage_response_1,
            lineage_response_2,
        ]

        # Expected result
        expected_lineage_links = {
            "target_1": ["source_1", "source_2", "source_1", "source_2"],
            "target_2": ["source_3", "source_3"],
        }

        # Call function and assert result
        result = generate_lineage_links(all_tables, lineage_client, project_id, regions)
        self.assertEqual(result, expected_lineage_links)


if __name__ == "__main__":
    unittest.main()
