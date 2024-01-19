import os
import sys
import unittest

sys.path.insert(0, os.path.abspath("../.."))


class TestEnvDefault(unittest.TestCase):
    def test_env_default(self) -> None:
        # hard to unit test
        pass


if __name__ == "__main__":
    unittest.main()
