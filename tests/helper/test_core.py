import os
import sys
import unittest
from typing import Any, Dict, List, Union

sys.path.insert(0, os.path.abspath("../.."))

from quollio_core.helper.core import new_global_id, setup_dbt_profile, trim_prefix
from quollio_core.repository.redshift import RedshiftConnectionConfig
from quollio_core.repository.snowflake import SnowflakeConnectionConfig


class TestHelper(unittest.TestCase):
    def test_new_global_id(self) -> None:
        test_cases: List[Dict[str, Union[str, Dict[str, str]]]] = [
            {
                "input": {
                    "tenant_id": "tenant1",
                    "endpoint": "mysql–instance1.123456789012.us-east-1.rds.amazonaws.com",
                    "data_id": "{db}{schema}{table}".format(
                        db="test_database", schema="test_schema", table="test_table"
                    ),
                    "asset_type": "table",
                },
                "expect": "tbl-d53b6e30f2c9405fac43f76b23833d61",
            },
            {
                "input": {
                    "tenant_id": "tenant1",
                    "endpoint": "snowflake-test-endpoint",
                    "data_id": "{db}{schema}{table}".format(db="SIMPSONS", schema="SIMPSONS_DATA", table="VOTES_RAW"),
                    "asset_type": "table",
                },
                "expect": "tbl-a6b74bc53229fe1c743a13762213f6d0",
            },
        ]

        for test_case in test_cases:
            input_value = test_case["input"]
            res = new_global_id(
                tenant_id=input_value["tenant_id"],
                cluster_id=input_value["endpoint"],
                data_id=input_value["data_id"],
                data_type=input_value["asset_type"],
            )
            self.assertEqual(res, test_case["expect"])

    def test_setup_dbt_profile(self):
        test_cases: List[Dict[str, Union[str, Dict[str, Any]]]] = [
            {
                "input": {
                    "sf_conn_config": RedshiftConnectionConfig(
                        host="test-host.amazonaws.com",
                        database="testdb",
                        schema="public",
                        build_user="build_user",
                        query_user="query_user",
                        build_password="BuildPassword",
                        query_password="QueryPassword",
                    ),
                    "template_content": """
quollio_intelligence_redshift:
  outputs:
    project:
      dbname: {{ database }}
      host: {{ host }}
      password: {{ build_password }}
      port: {{ port }}
      schema: {{ schema }}
      threads: {{ threads }}
      type: redshift
      user: {{ build_user }}
  target: project
""",
                },
                "expect": """quollio_intelligence_redshift:
  outputs:
    project:
      dbname: testdb
      host: test-host.amazonaws.com
      password: BuildPassword
      port: 5439
      schema: public
      threads: 3
      type: redshift
      user: build_user
  target: project
""",
            },
            {
                "input": {
                    "sf_conn_config": SnowflakeConnectionConfig(
                        account_id="test-endpoint",
                        account_build_role="TEST_BUILD_ROLE",
                        account_query_role="TEST_QUERY_ROLE",
                        account_user="TESTUSER",
                        account_password="TESTPASSWORD",
                        account_warehouse="TEST_WH",
                        account_database="QUOLLIO_DATA_PROFILER",
                        account_schema="PUBLIC",
                    ),
                    "template_content": """
quollio_intelligence_snowflake:
  outputs:
    project:
      account: {{ account_id }}
      database: {{ account_database }}
      role: {{ account_build_role }}
      schema: {{ account_schema }}
      type: snowflake
      user: {{ account_user }}
      password: {{ account_password }}
      warehouse: {{ account_warehouse }}
  target: project
""",
                },
                "expect": """quollio_intelligence_snowflake:
  outputs:
    project:
      account: test-endpoint
      database: QUOLLIO_DATA_PROFILER
      password: TESTPASSWORD
      role: TEST_BUILD_ROLE
      schema: PUBLIC
      type: snowflake
      user: TESTUSER
      warehouse: TEST_WH
  target: project
""",
            },
        ]
        template_path = "{cur_dir}/tests/helper".format(cur_dir=os.getcwd())
        template_name = "test_template.j2"
        template_file = f"{template_path}/{template_name}"
        profile_file = f"{template_path}/profiles.yml"
        for test_case in test_cases:
            input_value = test_case["input"]
            with open(template_file, "w") as f:
                f.write(input_value["template_content"])

            setup_dbt_profile(
                connections_json=input_value["sf_conn_config"].as_dict(),
                template_path=template_path,
                template_name=template_name,
            )

            with open(profile_file, "r") as f:
                content = f.read()
                self.assertEqual(content, test_case["expect"])

    def test_trim_prefix(self) -> None:
        test_cases: List[Dict[str, Union[str, Dict[str, str]]]] = [
            {
                "input": {
                    "s": "abc.com",
                    "prefix": "https://",
                },
                "expect": "abc.com",
            },
            {
                "input": {
                    "s": "https://abc.com",
                    "prefix": "https://",
                },
                "expect": "abc.com",
            },
        ]

        for test_case in test_cases:
            input_value = test_case["input"]
            res = trim_prefix(
                s=input_value["s"],
                prefix=input_value["prefix"],
            )
            self.assertEqual(res, test_case["expect"])


if __name__ == "__main__":
    unittest.main()
