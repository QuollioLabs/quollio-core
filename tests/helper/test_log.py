import logging
import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.abspath("../.."))

from quollio_core.helper import log


class TestSetLogLevel(unittest.TestCase):
    @patch("logging.basicConfig")
    def test_set_log_level_info(self, mock_basicConfig):
        log.set_log_level("info")
        mock_basicConfig.assert_called_once_with(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
        )

    @patch("logging.basicConfig")
    def test_set_log_level_debug(self, mock_basicConfig):
        log.set_log_level("debug")
        mock_basicConfig.assert_called_once_with(
            level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
        )

    @patch("logging.basicConfig")
    def test_set_log_level_warn(self, mock_basicConfig):
        log.set_log_level("warn")
        mock_basicConfig.assert_called_once_with(
            level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
        )

    @patch("logging.basicConfig")
    def test_set_log_level_error(self, mock_basicConfig):
        log.set_log_level("error")
        mock_basicConfig.assert_called_once_with(
            level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
        )

    @patch("logging.basicConfig")
    def test_set_log_level_critical(self, mock_basicConfig):
        log.set_log_level("critical")
        mock_basicConfig.assert_called_once_with(
            level=logging.CRITICAL, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
        )

    @patch("logging.basicConfig")
    def test_set_log_level_default(self, mock_basicConfig):
        log.set_log_level("unknown")
        mock_basicConfig.assert_called_once_with(
            level=logging.NOTSET, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
        )


if __name__ == "__main__":
    unittest.main()
