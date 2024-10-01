import unittest

from snowflake.connector import errors

from quollio_core.profilers.snowflake import handle_error


class TestSnowflakeProfilers(unittest.TestCase):
    def setUp(self):
        pass

    def test_handle_error(self):
        tests = [
            {
                "name": "permission error",
                "input": {
                    "error": errors.Error(
                        msg="test",
                        errno=2003,
                    ),
                },
                "expect": None,
            },
            {
                "name": "permission error",
                "input": {
                    "error": errors.Error(
                        msg="test",
                        errno=2037,
                    ),
                },
                "expect": None,
            },
            {
                "name": "Any other error",
                "input": {
                    "error": errors.Error(
                        msg="test",
                        errno=00000,
                    ),
                },
                "expect": Exception,
            },
            {
                "name": "Any other error",
                "input": {
                    "error": errors.Error(
                        msg="test",
                        errno=00000,
                    ),
                    "force_skip": True,
                },
                "expect": None,
            },
        ]
        for test in tests:
            if test.get("expect") is None:
                res = handle_error(err=test.get("input").get("error"), force_skip=test.get("input").get("force_skip"))
                self.assertEqual(res, test.get("expect"))

            if test.get("expect") is not None:
                with self.assertRaises(Exception):
                    res = handle_error(
                        err=test.get("input").get("error"), force_skip=test.get("input").get("force_skip")
                    )


if __name__ == "__main__":
    unittest.main()
