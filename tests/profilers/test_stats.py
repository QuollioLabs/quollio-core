import os
import sys
import unittest
from decimal import Decimal

sys.path.insert(0, os.path.abspath("../.."))

from quollio_core.helper.core import new_global_id
from quollio_core.profilers.stats import (
    ColumnStatsInput,
    StatsInput,
    StatsRequest,
    TableStatsInput,
    gen_table_stats_payload,
    gen_table_stats_payload_from_tuple,
)


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


class TestStats(unittest.TestCase):
    def setUp(self):
        pass

    def test_gen_table_stats_payload(self):
        test_cases = [
            {  # normal case
                "input": {
                    "tenant_id": "tenant1",
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
                            tenant_id="tenant1",
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
                tenant_id=test_input["tenant_id"], endpoint=test_input["ENDPOINT"], stats=test_input["STATS"]
            )
            self.assertEqual(res, test_case["expect"])

    def test_gen_table_stats_payload_from_tuple(self):
        test_cases = [
            {  # normal case
                "input": {
                    "tenant_id": "tenant1",
                    "ENDPOINT": "snowflake-test-endpoint",
                    "STATS": [
                        [
                            "TEST_DB1",  # db
                            "TEST_SCHEMA1",  # schema
                            "TEST_TABLE1",  # table
                            "TEST_COLUMN1",  # column
                            "10",  # max
                            "1",  # min
                            2,  # null cnt
                            3,  # cardinality
                            Decimal(4.2),  # avg
                            5,  # median
                            6,  # mode
                            7,  # stddev
                        ]
                    ],
                },
                "expect": [
                    StatsRequest(
                        global_id=new_global_id(
                            tenant_id="tenant1",
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
            res = gen_table_stats_payload_from_tuple(
                tenant_id=test_input["tenant_id"], endpoint=test_input["ENDPOINT"], stats=test_input["STATS"]
            )
            self.assertEqual(res, test_case["expect"])


if __name__ == "__main__":
    unittest.main()
