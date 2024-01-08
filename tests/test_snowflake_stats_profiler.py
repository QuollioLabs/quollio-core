import sys
import unittest
from decimal import Decimal

sys.path.append("quollio_core")
import snowflake_stats_profiler
from core.core import new_global_id
from profilers.stats_profiler import (
    ColumnStatsInput,
    StatsInput,
    StatsRequest,
    TableStatsInput,
    gen_table_stats_payload,
)


class TestStatsProfiler(unittest.TestCase):
    def setUp(self):
        pass

    def test__build_query(self):
        test_cases = [
            {
                "input": ["test-db1", "test-db2"],
                "expect": """
            SELECT
                DISTINCT
                TABLE_CATALOG
                , TABLE_SCHEMA
                , TABLE_NAME
            FROM
                QUOLLIO_DATA_PROFILER.PUBLIC.PROFILE_TARGET_COLUMNS
            WHERE
                TABLE_CATALOG in('test-db1', 'test-db2')
            """,
            },
            {
                "input": [],
                "expect": """
            SELECT
                DISTINCT
                TABLE_CATALOG
                , TABLE_SCHEMA
                , TABLE_NAME
            FROM
                QUOLLIO_DATA_PROFILER.PUBLIC.PROFILE_TARGET_COLUMNS
            """,
            },
        ]
        for test_case in test_cases:
            res = snowflake_stats_profiler._build_query(target_databases=test_case["input"])
            self.assertEqual(res, test_case["expect"])


class TestStatsInput(unittest.TestCase):
    def setUp(self):
        pass

    def test_get_column_stats(self):
        test_cases = [
            {  # normal case
                "input": StatsInput(
                    column_stats=ColumnStatsInput(
                        cardinality=3,
                        max="10",
                        mean=Decimal(4),
                        median=5,
                        min="1",
                        mode=6,
                        number_of_null=2,
                        number_of_unique=3,
                        stddev=7,
                    ),
                    table_stats=TableStatsInput(
                        count=0,
                        size=0.0,
                    ),
                ),
                "expect": {
                    "column_stats": {
                        "cardinality": 3,
                        "max": "10",
                        "mean": 4,
                        "median": 5,
                        "min": "1",
                        "mode": 6,
                        "number_of_null": 2,
                        "number_of_unique": 3,
                        "stddev": 7,
                    }
                },
            },
            {  # Decimal case
                "input": StatsInput(
                    column_stats=ColumnStatsInput(
                        cardinality=3,
                        max="10",
                        mean=Decimal(4),
                        median=5,
                        min="1",
                        mode=6,
                        number_of_null=2,
                        number_of_unique=3,
                        stddev=7,
                    ),
                    table_stats=TableStatsInput(
                        count=0,
                        size=0.0,
                    ),
                ),
                "expect": {
                    "column_stats": {
                        "cardinality": 3,
                        "max": "10",
                        "mean": 4,
                        "median": 5,
                        "min": "1",
                        "mode": 6,
                        "number_of_null": 2,
                        "number_of_unique": 3,
                        "stddev": 7,
                    }
                },
            },
        ]
        for test_case in test_cases:
            res = test_case["input"].get_column_stats()
            self.assertEqual(res, test_case["expect"])


class TestSnowflakeStatsProfiler(unittest.TestCase):
    def setUp(self):
        pass

    def test_gen_table_stats_payload(self):
        test_cases = [
            {  # normal case
                "input": {
                    "COMPANY_ID": "tenant1",
                    "ENDPOINT": "snowflake-test-endpoint",
                    "STATS": [
                        {
                            "DB_NAME": "TEST_DB1",
                            "SCHEMA_NAME": "TEST_SCHEMA1",
                            "TABLE_NAME": "TEST_TABLE1",
                            "COLUMN_NAME": "TEST_COLUMN1",
                            "MAX_VALUE": "10",
                            "MIN_VALUE": "1",
                            "NULL_COUNT": 2,
                            "CARDINALITY": 3,
                            "AVG_VALUE": Decimal(4.2),
                            "MEDIAN_VALUE": 5,
                            "MODE_VALUE": 6,
                            "STDDEV_VALUE": 7,
                        },
                    ],
                },
                "expect": [
                    StatsRequest(
                        global_id=new_global_id(
                            company_id="tenant1",
                            cluster_id="snowflake-test-endpoint",
                            data_id="TEST_DB1TEST_SCHEMA1TEST_TABLE1TEST_COLUMN1",
                            data_type="column",
                        ),
                        db="TEST_DB1",
                        schema="TEST_SCHEMA1",
                        table="TEST_TABLE1",
                        column="TEST_COLUMN1",
                        body=StatsInput(
                            column_stats=ColumnStatsInput(
                                cardinality=3,
                                max="10",
                                mean="4.2",
                                median="5",
                                min="1",
                                mode="6",
                                number_of_null=2,
                                number_of_unique=3,
                                stddev="7",
                            ),
                            table_stats=TableStatsInput(
                                count=0,
                                size=0.0,
                            ),
                        ),
                    )
                ],
            },
        ]
        for test_case in test_cases:
            test_input = test_case["input"]
            res = gen_table_stats_payload(
                company_id=test_input["COMPANY_ID"], endpoint=test_input["ENDPOINT"], stats=test_input["STATS"]
            )
            self.assertEqual(res, test_case["expect"])


if __name__ == "__main__":
    unittest.main()
