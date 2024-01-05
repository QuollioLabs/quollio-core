import sys
import unittest

sys.path.append("quollio_core")

from core.core import new_global_id
from profilers.lineage_profiler import (
    LineageInput,
    LineageInputs,
    gen_column_lineage_payload,
    gen_table_lineage_payload,
    gen_table_lineage_payload_inputs,
    parse_snowflake_results,
)


class TestSnowflakeLineageProfiler(unittest.TestCase):
    def setUp(self):
        pass

    def test_gen_table_lineage_payload(self):
        test_cases = [
            {  # normal case
                "input": {
                    "COMPANY_ID": "tenant1",
                    "ENDPOINT": "snowflake-test-endpoint",
                    "TABLE_INPUT": [
                        {
                            "DOWNSTREAM_TABLE_NAME": "TEST_DB1.TEST_SCHEMA1.TEST_TABLE1",
                            "DOWNSTREAM_TABLE_DOMAIN": "Table",
                            "UPSTREAM_TABLES": [
                                {
                                    "upstream_object_domain": "Stage",
                                    "upstream_object_name": "TEST_DB2.TEST_SCHEMA1.TEST_TABLE1",
                                },
                                {
                                    "upstream_object_domain": "Stage",
                                    "upstream_object_name": "TEST_DB2.TEST_SCHEMA1.TEST_TABLE2",
                                },
                            ],
                        },
                    ],
                },
                "expect": [
                    LineageInputs(
                        downstream_global_id=new_global_id(
                            company_id="tenant1",
                            cluster_id="snowflake-test-endpoint",
                            data_id="TEST_DB1TEST_SCHEMA1TEST_TABLE1",
                            data_type="table",
                        ),
                        downstream_database_name="TEST_DB1",
                        downstream_schema_name="TEST_SCHEMA1",
                        downstream_table_name="TEST_TABLE1",
                        downstream_column_name="",
                        upstreams=LineageInput(
                            upstream=[
                                new_global_id(
                                    company_id="tenant1",
                                    cluster_id="snowflake-test-endpoint",
                                    data_id="TEST_DB2TEST_SCHEMA1TEST_TABLE1",
                                    data_type="table",
                                ),
                                new_global_id(
                                    company_id="tenant1",
                                    cluster_id="snowflake-test-endpoint",
                                    data_id="TEST_DB2TEST_SCHEMA1TEST_TABLE2",
                                    data_type="table",
                                ),
                            ]
                        ),
                    ),
                ],
            },
            {  # null case
                "input": {
                    "COMPANY_ID": "tenant1",
                    "ENDPOINT": "snowflake-test-endpoint",
                    "TABLE_INPUT": [],
                },
                "expect": [],
            },
            {  # skip a downstream table name doesn't consist of db schema and table.
                "input": {
                    "COMPANY_ID": "tenant1",
                    "ENDPOINT": "snowflake-test-endpoint",
                    "TABLE_INPUT": [
                        {
                            "DOWNSTREAM_TABLE_NAME": "TEST_DB1.TEST_SCHEMA1.TEST_TABLE1",
                            "DOWNSTREAM_TABLE_DOMAIN": "Table",
                            "UPSTREAM_TABLES": [
                                {
                                    "upstream_object_domain": "Stage",
                                    "upstream_object_name": "TEST_DB2.TEST_SCHEMA1.TEST_TABLE1",
                                }
                            ],
                        },
                        {
                            "DOWNSTREAM_TABLE_NAME": "TEST_DB1",
                            "DOWNSTREAM_TABLE_DOMAIN": "Table",
                            "UPSTREAM_TABLES": [
                                {
                                    "upstream_object_domain": "Stage",
                                    "upstream_object_name": "TEST_DB2.TEST_SCHEMA1.TEST_TABLE1",
                                },
                                {
                                    "upstream_object_domain": "Stage",
                                    "upstream_object_name": "TEST_DB2.TEST_SCHEMA1.TEST_TABLE2",
                                },
                            ],
                        },
                    ],
                },
                "expect": [
                    LineageInputs(
                        downstream_global_id=new_global_id(
                            company_id="tenant1",
                            cluster_id="snowflake-test-endpoint",
                            data_id="TEST_DB1TEST_SCHEMA1TEST_TABLE1",
                            data_type="table",
                        ),
                        downstream_database_name="TEST_DB1",
                        downstream_schema_name="TEST_SCHEMA1",
                        downstream_table_name="TEST_TABLE1",
                        downstream_column_name="",
                        upstreams=LineageInput(
                            upstream=[
                                new_global_id(
                                    company_id="tenant1",
                                    cluster_id="snowflake-test-endpoint",
                                    data_id="TEST_DB2TEST_SCHEMA1TEST_TABLE1",
                                    data_type="table",
                                ),
                            ]
                        ),
                    ),
                ],
            },
            {  # skip an upstream table name doesn't consist of db schema and table.
                "input": {
                    "COMPANY_ID": "tenant1",
                    "ENDPOINT": "snowflake-test-endpoint",
                    "TABLE_INPUT": [
                        {
                            "DOWNSTREAM_TABLE_NAME": "TEST_DB1.TEST_SCHEMA1.TEST_TABLE1",
                            "DOWNSTREAM_TABLE_DOMAIN": "Table",
                            "UPSTREAM_TABLES": [
                                {
                                    "upstream_object_domain": "Stage",
                                    "upstream_object_name": "TEST_DB2.TEST_SCHEMA1.TEST_TABLE1",
                                },
                                {"upstream_object_domain": "Stage", "upstream_object_name": "TEST_DB2"},
                            ],
                        },
                    ],
                },
                "expect": [
                    LineageInputs(
                        downstream_global_id=new_global_id(
                            company_id="tenant1",
                            cluster_id="snowflake-test-endpoint",
                            data_id="TEST_DB1TEST_SCHEMA1TEST_TABLE1",
                            data_type="table",
                        ),
                        downstream_database_name="TEST_DB1",
                        downstream_schema_name="TEST_SCHEMA1",
                        downstream_table_name="TEST_TABLE1",
                        downstream_column_name="",
                        upstreams=LineageInput(
                            upstream=[
                                new_global_id(
                                    company_id="tenant1",
                                    cluster_id="snowflake-test-endpoint",
                                    data_id="TEST_DB2TEST_SCHEMA1TEST_TABLE1",
                                    data_type="table",
                                ),
                            ]
                        ),
                    ),
                ],
            },
        ]
        for test_case in test_cases:
            test_input = test_case["input"]
            res = gen_table_lineage_payload(
                company_id=test_input["COMPANY_ID"], endpoint=test_input["ENDPOINT"], tables=test_input["TABLE_INPUT"]
            )
            self.assertEqual(res, test_case["expect"])

    def test_gen_column_lineage_payload(self):
        test_cases = [
            {  # normal case
                "input": {
                    "COMPANY_ID": "tenant1",
                    "ENDPOINT": "snowflake-test-endpoint",
                    "COLUMN_INPUT": [
                        {
                            "DOWNSTREAM_TABLE_NAME": "TEST_DB1.TEST_SCHEMA1.TEST_TABLE1",
                            "DOWNSTREAM_COLUMN_NAME": "TEST_COLUMN1",
                            "UPSTREAM_COLUMNS": '[\n  {\n    "upstream_object_domain": "Stage",\n    "upstream_table_name": "TEST_DB2.TEST_SCHEMA1.TEST_TABLE1",\n    "upstream_column_name": "TEST_COLUMN1"  }, {\n    "upstream_object_domain": "Stage",\n    "upstream_table_name": "TEST_DB3.TEST_SCHEMA1.TEST_TABLE1",\n    "upstream_column_name": "TEST_COLUMN2"  }\n]',  # NOQA
                        },
                    ],
                },
                "expect": [
                    LineageInputs(
                        downstream_global_id=new_global_id(
                            company_id="tenant1",
                            cluster_id="snowflake-test-endpoint",
                            data_id="TEST_DB1TEST_SCHEMA1TEST_TABLE1TEST_COLUMN1",
                            data_type="column",
                        ),
                        downstream_database_name="TEST_DB1",
                        downstream_schema_name="TEST_SCHEMA1",
                        downstream_table_name="TEST_TABLE1",
                        downstream_column_name="TEST_COLUMN1",
                        upstreams=LineageInput(
                            upstream=[
                                new_global_id(
                                    company_id="tenant1",
                                    cluster_id="snowflake-test-endpoint",
                                    data_id="TEST_DB2TEST_SCHEMA1TEST_TABLE1TEST_COLUMN1",
                                    data_type="column",
                                ),
                                new_global_id(
                                    company_id="tenant1",
                                    cluster_id="snowflake-test-endpoint",
                                    data_id="TEST_DB3TEST_SCHEMA1TEST_TABLE1TEST_COLUMN2",
                                    data_type="column",
                                ),
                            ]
                        ),
                    ),
                ],
            },
            {  # null case
                "input": {
                    "COMPANY_ID": "tenant1",
                    "ENDPOINT": "snowflake-test-endpoint",
                    "COLUMN_INPUT": [],
                },
                "expect": [],
            },
            {  # skip an downstream table name doesn't consist of db schema and table.
                "input": {
                    "COMPANY_ID": "tenant1",
                    "ENDPOINT": "snowflake-test-endpoint",
                    "COLUMN_INPUT": [
                        {
                            "DOWNSTREAM_TABLE_NAME": "TEST_DB1.TEST_SCHEMA1.TEST_TABLE1",
                            "DOWNSTREAM_COLUMN_NAME": "TEST_COLUMN1",
                            "UPSTREAM_COLUMNS": '[\n  {\n    "upstream_object_domain": "Stage",\n    "upstream_table_name": "TEST_DB2.TEST_SCHEMA1.TEST_TABLE1",\n    "upstream_column_name": "TEST_COLUMN1"  }, {\n    "upstream_object_domain": "Stage",\n    "upstream_table_name": "TEST_DB3.TEST_SCHEMA1.TEST_TABLE1",\n    "upstream_column_name": "TEST_COLUMN2"  }\n]',  # NOQA
                        },
                        {
                            "DOWNSTREAM_TABLE_NAME": "TEST_DB1",
                            "DOWNSTREAM_COLUMN_NAME": "TEST_COLUMN2",
                            "UPSTREAM_COLUMNS": '[\n  {\n    "upstream_object_domain": "Stage",\n    "upstream_table_name": "TEST_DB2.TEST_SCHEMA1.TEST_TABLE1",\n    "upstream_column_name": "TEST_COLUMN1"  }, {\n    "upstream_object_domain": "Stage",\n    "upstream_table_name": "TEST_DB3.TEST_SCHEMA1.TEST_TABLE1",\n    "upstream_column_name": "TEST_COLUMN2"  }\n]',  # NOQA
                        },
                    ],
                },
                "expect": [
                    LineageInputs(
                        downstream_global_id=new_global_id(
                            company_id="tenant1",
                            cluster_id="snowflake-test-endpoint",
                            data_id="TEST_DB1TEST_SCHEMA1TEST_TABLE1TEST_COLUMN1",
                            data_type="column",
                        ),
                        downstream_database_name="TEST_DB1",
                        downstream_schema_name="TEST_SCHEMA1",
                        downstream_table_name="TEST_TABLE1",
                        downstream_column_name="TEST_COLUMN1",
                        upstreams=LineageInput(
                            upstream=[
                                new_global_id(
                                    company_id="tenant1",
                                    cluster_id="snowflake-test-endpoint",
                                    data_id="TEST_DB2TEST_SCHEMA1TEST_TABLE1TEST_COLUMN1",
                                    data_type="column",
                                ),
                                new_global_id(
                                    company_id="tenant1",
                                    cluster_id="snowflake-test-endpoint",
                                    data_id="TEST_DB3TEST_SCHEMA1TEST_TABLE1TEST_COLUMN2",
                                    data_type="column",
                                ),
                            ]
                        ),
                    ),
                ],
            },
            {  # skip an upstream table name doesn't consist of db schema and table.
                "input": {
                    "COMPANY_ID": "tenant1",
                    "ENDPOINT": "snowflake-test-endpoint",
                    "COLUMN_INPUT": [
                        {
                            "DOWNSTREAM_TABLE_NAME": "TEST_DB1.TEST_SCHEMA1.TEST_TABLE1",
                            "DOWNSTREAM_COLUMN_NAME": "TEST_COLUMN1",
                            "UPSTREAM_COLUMNS": '[\n  {\n    "upstream_object_domain": "Stage",\n    "upstream_table_name": "TEST_DB2.TEST_SCHEMA1.TEST_TABLE1",\n    "upstream_column_name": "TEST_COLUMN1"  }, {\n    "upstream_object_domain": "Stage",\n    "upstream_table_name": "TEST_DB3",\n    "upstream_column_name": "TEST_COLUMN2"  }\n]',  # NOQA
                        },
                    ],
                },
                "expect": [
                    LineageInputs(
                        downstream_global_id=new_global_id(
                            company_id="tenant1",
                            cluster_id="snowflake-test-endpoint",
                            data_id="TEST_DB1TEST_SCHEMA1TEST_TABLE1TEST_COLUMN1",
                            data_type="column",
                        ),
                        downstream_database_name="TEST_DB1",
                        downstream_schema_name="TEST_SCHEMA1",
                        downstream_table_name="TEST_TABLE1",
                        downstream_column_name="TEST_COLUMN1",
                        upstreams=LineageInput(
                            upstream=[
                                new_global_id(
                                    company_id="tenant1",
                                    cluster_id="snowflake-test-endpoint",
                                    data_id="TEST_DB2TEST_SCHEMA1TEST_TABLE1TEST_COLUMN1",
                                    data_type="column",
                                ),
                            ]
                        ),
                    ),
                ],
            },
            {  # skip an upstream column name doesn't exist.
                "input": {
                    "COMPANY_ID": "tenant1",
                    "ENDPOINT": "snowflake-test-endpoint",
                    "COLUMN_INPUT": [
                        {
                            "DOWNSTREAM_TABLE_NAME": "TEST_DB1.TEST_SCHEMA1.TEST_TABLE1",
                            "DOWNSTREAM_COLUMN_NAME": "TEST_COLUMN1",
                            "UPSTREAM_COLUMNS": '[\n  {\n    "upstream_object_domain": "Stage",\n    "upstream_table_name": "TEST_DB2.TEST_SCHEMA1.TEST_TABLE1",\n    "upstream_column_name": "TEST_COLUMN1"  }, {\n    "upstream_object_domain": "Stage",\n    "upstream_table_name": "TEST_DB3.TEST_SCHEMA1.TEST_TABLE1"    }\n]',  # NOQA
                        },
                    ],
                },
                "expect": [
                    LineageInputs(
                        downstream_global_id=new_global_id(
                            company_id="tenant1",
                            cluster_id="snowflake-test-endpoint",
                            data_id="TEST_DB1TEST_SCHEMA1TEST_TABLE1TEST_COLUMN1",
                            data_type="column",
                        ),
                        downstream_database_name="TEST_DB1",
                        downstream_schema_name="TEST_SCHEMA1",
                        downstream_table_name="TEST_TABLE1",
                        downstream_column_name="TEST_COLUMN1",
                        upstreams=LineageInput(
                            upstream=[
                                new_global_id(
                                    company_id="tenant1",
                                    cluster_id="snowflake-test-endpoint",
                                    data_id="TEST_DB2TEST_SCHEMA1TEST_TABLE1TEST_COLUMN1",
                                    data_type="column",
                                ),
                            ]
                        ),
                    ),
                ],
            },
        ]
        for test_case in test_cases:
            test_input = test_case["input"]
            res = gen_column_lineage_payload(
                company_id=test_input["COMPANY_ID"], endpoint=test_input["ENDPOINT"], columns=test_input["COLUMN_INPUT"]
            )
            self.assertEqual(res, test_case["expect"])

    def test_gen_lineage_payload_inputs(self):
        test_cases = [
            {
                "input": (
                    ["dev.public.table_dest1", "dev.public.table_src1"],
                    ["dev.public.table_dest2", "dev.public.table_src3"],
                    ["dev.public.table_dest1", "dev.public.table_src2"],
                    ["dev.public.table_dest2", "dev.public.table_src4"],
                ),
                "expect": [
                    {
                        "DOWNSTREAM_TABLE_NAME": "dev.public.table_dest1",
                        "UPSTREAM_TABLES": [
                            {
                                "upstream_object_name": "dev.public.table_src1",
                            },
                            {
                                "upstream_object_name": "dev.public.table_src2",
                            },
                        ],
                    },
                    {
                        "DOWNSTREAM_TABLE_NAME": "dev.public.table_dest2",
                        "UPSTREAM_TABLES": [
                            {
                                "upstream_object_name": "dev.public.table_src3",
                            },
                            {
                                "upstream_object_name": "dev.public.table_src4",
                            },
                        ],
                    },
                ],
            }
        ]
        for test_case in test_cases:
            res = gen_table_lineage_payload_inputs(test_case["input"])
            self.assertEqual(res, test_case["expect"])

    def test_parse_snowflake_results(self):
        test_cases = [
            {
                "input": (
                    [
                        {
                            "DOWNSTREAM_TABLE_NAME": "TEST_DB1.TEST_SCHEMA1.TEST_TABLE1",
                            "DOWNSTREAM_TABLE_DOMAIN": "Table",
                            "UPSTREAM_TABLES": '[\n  {\n    "upstream_object_domain": "Stage",\n    "upstream_object_name": "TEST_DB2.TEST_SCHEMA1.TEST_TABLE1"\n  }, {\n    "upstream_object_domain": "Stage",\n    "upstream_object_name": "TEST_DB2.TEST_SCHEMA1.TEST_TABLE2"\n  }\n]',  # NOQA
                        },
                    ]
                ),
                "expect": [
                    {
                        "DOWNSTREAM_TABLE_NAME": "TEST_DB1.TEST_SCHEMA1.TEST_TABLE1",
                        "DOWNSTREAM_TABLE_DOMAIN": "Table",
                        "UPSTREAM_TABLES": [
                            {
                                "upstream_object_domain": "Stage",
                                "upstream_object_name": "TEST_DB2.TEST_SCHEMA1.TEST_TABLE1",
                            },
                            {
                                "upstream_object_domain": "Stage",
                                "upstream_object_name": "TEST_DB2.TEST_SCHEMA1.TEST_TABLE2",
                            },
                        ],
                    },
                ],
            }
        ]
        for test_case in test_cases:
            res = parse_snowflake_results(test_case["input"])
            self.assertEqual(res, test_case["expect"])


if __name__ == "__main__":
    unittest.main()
