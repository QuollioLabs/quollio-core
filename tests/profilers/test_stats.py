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
    get_column_stats_items,
    get_is_target_stats_items,
    render_sql_for_stats,
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
            {  # normal case
                "input": {
                    "tenant_id": "tenant1",
                    "ENDPOINT": "snowflake-test-endpoint",
                    "STATS": [
                        {
                            "db_name": "TEST_DB1",
                            "schema_name": "TEST_SCHEMA1",
                            "table_name": "TEST_TABLE2",
                            "column_name": "TEST_COLUMN2",
                            "max_value": "10",
                            "min_value": "1",
                            "null_count": 2,
                            "cardinality": 3,
                            "avg_value": Decimal(4.2),
                            "median_value": 5,
                            "mode_value": 6,
                            "stddev_value": 7,
                        },
                    ],
                },
                "expect": [
                    StatsRequest(
                        global_id=new_global_id(
                            tenant_id="tenant1",
                            cluster_id="snowflake-test-endpoint",
                            data_id="TEST_DB1TEST_SCHEMA1TEST_TABLE2TEST_COLUMN2",
                            data_type="column",
                        ),
                        db="TEST_DB1",
                        schema="TEST_SCHEMA1",
                        table="TEST_TABLE2",
                        column="TEST_COLUMN2",
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

    def test_render_sql_for_stats(self):
        test_cases = [
            {
                "input": {
                    "stats_items": {
                        "cardinality": True,
                        "max": True,
                        "mean": True,
                        "median": True,
                        "min": True,
                        "mode": True,
                        "number_of_null": True,
                        "number_of_unique": True,
                        "stddev": True,
                    },
                    "table_fqn": "test_db.test_schema.test_table1",
                },
                "expect": """
                    SELECT
                        db_name
                        , schema_name
                        , table_name
                        , column_name
                        , max_value
                        , min_value
                        , null_count
                        , cardinality
                        , avg_value
                        , median_value
                        , mode_value
                        , stddev_value
                    FROM
                        test_db.test_schema.test_table1
                """,
            },
            {
                "input": {
                    "stats_items": {
                        "cardinality": False,
                        "max": False,
                        "mean": False,
                        "median": False,
                        "min": False,
                        "mode": False,
                        "number_of_null": False,
                        "number_of_unique": False,
                        "stddev": False,
                    },
                    "table_fqn": "test_db.test_schema.test_table2",
                },
                "expect": """
                    SELECT
                        db_name
                        , schema_name
                        , table_name
                        , column_name
                        , null as max_value
                        , null as min_value
                        , null as null_count
                        , null as cardinality
                        , null as avg_value
                        , null as median_value
                        , null as mode_value
                        , null as stddev_value
                    FROM
                        test_db.test_schema.test_table2
                """,
            },
            {
                "input": {
                    "stats_items": {
                        "cardinality": True,
                        "max": True,
                        "mean": True,
                        "median": True,
                        "min": True,
                        "mode": True,
                        "number_of_null": True,
                        "number_of_unique": True,
                        "stddev": True,
                    },
                    "table_fqn": "sub_table",
                    "cte": """with t1 as (
                              select
                                *
                              from
                              sub_table
                            )""",
                },
                "expect": """
                    with t1 as (
                      select
                        *
                      from
                        sub_table
                    )
                    SELECT
                        db_name
                        , schema_name
                        , table_name
                        , column_name
                        , max_value
                        , min_value
                        , null_count
                        , cardinality
                        , avg_value
                        , median_value
                        , mode_value
                        , stddev_value
                    FROM
                        sub_table
                """,
            },
        ]
        for test_case in test_cases:
            test_input = test_case["input"]
            res = render_sql_for_stats(
                is_aggregate_items=test_input["stats_items"],
                table_fqn=test_input["table_fqn"],
                cte=test_input.get("cte"),
            )
            self.assertEqual(self.normalize_whitespace(res), self.normalize_whitespace(test_case["expect"]))

    def test_get_is_target_stats_items(self):
        test_cases = [
            {
                "input": {
                    "stats_items": [],
                },
                "expect": {
                    "cardinality": False,
                    "max": False,
                    "mean": False,
                    "median": False,
                    "min": False,
                    "mode": False,
                    "number_of_null": False,
                    "number_of_unique": False,
                    "stddev": False,
                },
            },
            {
                "input": {
                    "stats_items": ["max"],
                },
                "expect": {
                    "cardinality": False,
                    "max": True,
                    "mean": False,
                    "median": False,
                    "min": False,
                    "mode": False,
                    "number_of_null": False,
                    "number_of_unique": False,
                    "stddev": False,
                },
            },
            {  # normal case
                "input": {
                    "stats_items": [
                        "cardinality",
                        "max",
                        "mean",
                        "median",
                        "min",
                        "mode",
                        "number_of_null",
                        "number_of_unique",
                        "stddev",
                    ],
                },
                "expect": {
                    "cardinality": True,
                    "max": True,
                    "mean": True,
                    "median": True,
                    "min": True,
                    "mode": True,
                    "number_of_null": True,
                    "number_of_unique": True,
                    "stddev": True,
                },
            },
        ]
        for test_case in test_cases:
            test_input = test_case["input"]
            res = get_is_target_stats_items(
                stats_items=test_input["stats_items"],
            )
            self.assertEqual(res, test_case["expect"])

    # To ignore whitespace when calling assertion.
    @staticmethod
    def normalize_whitespace(s: str) -> str:
        return " ".join(s.split())

    def test_get_column_stats_items(self):
        expect = ["cardinality", "max", "mean", "median", "min", "mode", "number_of_null", "number_of_unique", "stddev"]
        res = get_column_stats_items()
        self.assertEqual(res, expect)


if __name__ == "__main__":
    unittest.main()
