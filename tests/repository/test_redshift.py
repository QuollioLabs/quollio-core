import os
import sys
import unittest

sys.path.insert(0, os.path.abspath("../.."))

from quollio_core.repository.redshift import RedshiftConnectionConfig, RedshiftErrorCode


class TestRedshiftErrorCode(unittest.TestCase):
    def setUp(self):
        pass

    def test_redshift_error_code_normal(self):
        test_cases = [
            {
                "input": {
                    "error_code": "08000",
                },
                "expect": {
                    "name": "CONNECTION_EXCEPTION",
                    "code": "08000",
                },
            }
        ]
        for test_case in test_cases:
            enum = RedshiftErrorCode(test_case["input"]["error_code"])
            self.assertEqual(enum.name, test_case["expect"]["name"])
            self.assertEqual(enum.value, test_case["expect"]["code"])

            match_error_code = RedshiftErrorCode.match_error_code(test_case["input"]["error_code"])
            self.assertEqual(match_error_code, True)

    def test_redshift_error_code_error(self):
        test_case = "dummy"
        with self.assertRaises(ValueError):
            RedshiftErrorCode(test_case)

        match_error_code = RedshiftErrorCode.match_error_code(test_case)
        self.assertEqual(match_error_code, False)


class TestRedshiftConnectionConfig(unittest.TestCase):
    def test_redshift_connection_config(self):
        test_cases = [
            {
                "input": {
                    "host": "test-host.amazonaws.com",
                    "port": 5439,
                    "database": "testdb",
                    "schema": "public",
                    "build_user": "build_user",
                    "query_user": "query_user",
                    "build_password": "BuildPassword",
                    "query_password": "QueryPassword",
                },
                "expect": {
                    "host": "test-host.amazonaws.com",
                    "port": 5439,
                    "database": "testdb",
                    "schema": "public",
                    "build_user": "build_user",
                    "query_user": "query_user",
                    "build_password": "BuildPassword",
                    "query_password": "QueryPassword",
                    "threads": 3,
                },
            },
            {  # Non default values are used.
                "input": {
                    "host": "test-host.amazonaws.com",
                    "port": 5439,
                    "database": "testdb",
                    "schema": "public",
                    "build_user": "build_user",
                    "query_user": "query_user",
                    "build_password": "BuildPassword",
                    "query_password": "QueryPassword",
                    "threads": 5,
                },
                "expect": {
                    "host": "test-host.amazonaws.com",
                    "port": 5439,
                    "database": "testdb",
                    "schema": "public",
                    "build_user": "build_user",
                    "query_user": "query_user",
                    "build_password": "BuildPassword",
                    "query_password": "QueryPassword",
                    "threads": 5,
                },
            },
        ]
        for test_case in test_cases:
            res = RedshiftConnectionConfig(
                host=test_case["input"]["host"],
                port=test_case["input"]["port"],
                database=test_case["input"]["database"],
                schema=test_case["input"]["schema"],
                build_user=test_case["input"]["build_user"],
                query_user=test_case["input"]["query_user"],
                build_password=test_case["input"]["build_password"],
                query_password=test_case["input"]["query_password"],
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
