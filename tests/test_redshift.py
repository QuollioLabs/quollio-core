import sys
import unittest

sys.path.append("quollio_core")

from repository.redshift import RedshiftConnectionConfig


class TestRedshiftConnectionConfig(unittest.TestCase):
    def test_redshift_connection_config(self):
        test_cases = [
            {
                "input": {
                    "host": "test-host.amazonaws.com",
                    "database": "testdb",
                    "user": "user",
                    "password": "Password",
                },
                "expect": {
                    "host": "test-host.amazonaws.com",
                    "database": "testdb",
                    "user": "user",
                    "password": "Password",
                    "schema": "public",
                    "port": 5439,
                    "threads": 2,
                },
            },
            {  # Non default values are used.
                "input": {
                    "host": "test-host.amazonaws.com",
                    "database": "testdb",
                    "user": "user",
                    "password": "Password",
                    "schema": "dummy",
                    "port": 1234,
                    "threads": 3,
                },
                "expect": {
                    "host": "test-host.amazonaws.com",
                    "database": "testdb",
                    "user": "user",
                    "password": "Password",
                    "schema": "dummy",
                    "port": 1234,
                    "threads": 3,
                },
            },
        ]
        for test_case in test_cases:
            res = RedshiftConnectionConfig(
                host=test_case["input"]["host"],
                database=test_case["input"]["database"],
                user=test_case["input"]["user"],
                password=test_case["input"]["password"],
            )
            if test_case["input"].get("schema"):
                res.schema = test_case["input"].get("schema")

            if test_case["input"].get("port"):
                res.port = test_case["input"].get("port")

            if test_case["input"].get("threads"):
                res.threads = test_case["input"].get("threads")

            self.assertEqual(res.as_dict(), test_case["expect"])


if __name__ == "__main__":
    unittest.main()
