import argparse
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath("../.."))

from quollio_core.helper.env_default import env_default


class TestEnvDefault(unittest.TestCase):
    def setUp(self):
        os.environ["TEST_SIMPLE_STRING"] = "test from env"
        os.environ["TEST_LIST1"] = "test_from_env1"
        os.environ["TEST_LIST2"] = "test_from_env1 test_from_env2"
        os.environ["TEST_LIST3"] = "test_from_env1 test_from_env2"
        os.environ["TEST_BOOL"] = "False"

    def tearDown(self):
        del os.environ["TEST_SIMPLE_STRING"]
        del os.environ["TEST_LIST1"]
        del os.environ["TEST_LIST2"]
        del os.environ["TEST_LIST3"]
        del os.environ["TEST_BOOL"]

    def test_env_default_simple_string(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--test_simple_string",
            action=env_default("TEST_SIMPLE_STRING"),
            required=False,
        )
        args = parser.parse_args([])
        self.assertEqual(args.test_simple_string, "test from env")

        args = parser.parse_args(["--test_simple_string", "test from args"])
        self.assertEqual(args.test_simple_string, "test from args")

    def test_env_default_list1(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--test_list1",
            action=env_default("TEST_LIST1"),
            nargs="*",
            required=False,
        )
        args = parser.parse_args([])
        self.assertEqual(args.test_list1, ["test_from_env1"])

        args = parser.parse_args(["--test_list1", "test_from_arg1"])
        self.assertEqual(args.test_list1, ["test_from_arg1"])

    def test_env_default_list2(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--test_list2",
            action=env_default("TEST_LIST2"),
            nargs="*",
            required=False,
        )
        args = parser.parse_args([])
        self.assertEqual(args.test_list2, ["test_from_env1", "test_from_env2"])

        args = parser.parse_args(["--test_list2", "test_from_arg1", "test_from_arg2"])
        self.assertEqual(args.test_list2, ["test_from_arg1", "test_from_arg2"])

    def test_env_default_list_using_plus(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--test_list_using_plus",
            action=env_default("TEST_LIST3"),
            nargs="+",
            choices=["test_from_env1", "test_from_env2", "test_from_arg1", "test_from_arg2"],
            required=False,
        )
        args = parser.parse_args([])
        self.assertEqual(args.test_list_using_plus, ["test_from_env1", "test_from_env2"])

        args = parser.parse_args(["--test_list_using_plus", "test_from_arg1", "test_from_arg2"])
        self.assertEqual(args.test_list_using_plus, ["test_from_arg1", "test_from_arg2"])

        with self.assertRaises(SystemExit):  # MEMO: Need to use value in choice array.
            args = parser.parse_args(["--test_list_using_plus", "test_from_arg1", "test_from_arg3"])

    def test_env_default_bool(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--test_bool",
            type=bool,
            action=env_default("TEST_BOOL", store_true=True),
            default=False,
            required=False,
        )
        args = parser.parse_args([])
        self.assertEqual(args.test_bool, False)

        args = parser.parse_args(["--test_bool"])
        self.assertEqual(args.test_bool, True)

    def test_env_default_bool_default_true(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--test_bool",
            type=bool,
            action=env_default("TEST_BOOL1", store_true=True),
            default=True,
            required=False,
        )
        args = parser.parse_args([])
        self.assertEqual(args.test_bool, True)

        args = parser.parse_args(["--test_bool"])
        self.assertEqual(args.test_bool, True)


if __name__ == "__main__":
    unittest.main()
