import sys
import unittest
from typing import Dict, List, Union

sys.path.append("quollio_core")

from core.core import new_global_id


class TestCore(unittest.TestCase):
    def test_new_global_id(self) -> None:
        test_cases: List[Dict[str, Union[str, Dict[str, str]]]] = [
            {
                "input": {
                    "company_id": "tenant1",
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
                    "company_id": "tenant1",
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
                company_id=input_value["company_id"],
                cluster_id=input_value["endpoint"],
                data_id=input_value["data_id"],
                data_type=input_value["asset_type"],
            )
            self.assertEqual(res, test_case["expect"])


if __name__ == "__main__":
    unittest.main()
