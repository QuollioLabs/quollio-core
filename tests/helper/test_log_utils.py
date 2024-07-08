import logging
import unittest

from quollio_core.helper.log_utils import configure_logging, error_handling_decorator


# Sample function to test the decorator
@error_handling_decorator
def test_function(success: bool):
    logger = logging.getLogger(__name__)
    if success:
        logger.info("Test function executed successfully.")
        return "Success!"
    else:
        raise ValueError("An error occurred in the test function.")


class TestLogUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Configure logging
        configure_logging("debug")

    def test_successful_execution(self):
        """Test the function when it succeeds."""
        result = test_function(True)
        self.assertEqual(result, "Success!")

    def test_error_execution(self):
        """Test the function when it raises an error."""
        with self.assertRaises(ValueError):
            test_function(False)


if __name__ == "__main__":
    unittest.main()
