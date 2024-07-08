import inspect
import logging

LOG_LEVELS = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
    "notset": logging.NOTSET,
}

logger = logging.getLogger(__name__)


def configure_logging(level: str = "INFO"):
    """Configure logging settings."""
    log_level = LOG_LEVELS.get(level.lower())
    if log_level is None:
        raise ValueError(f"Unknown log level: {level}")

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s",
    )
    logger.setLevel(log_level)
    logger.info(f"Logging is configured to {level} level.")


def error_handling_decorator(func):
    """Decorator for consistent error handling in CLI commands."""

    def wrapper(*args, **kwargs):
        func_name = func.__name__
        try:
            logger.debug(f"Starting {func_name}")
            result = func(*args, **kwargs)
            logger.debug(f"Completed {func_name} successfully")
            return result
        except Exception as e:
            current_frame = inspect.currentframe()
            error_frame = current_frame.f_back
            line_number = error_frame.f_lineno
            logger.error(f"Error in {func_name} at line {line_number}: {str(e)}", exc_info=True)
            raise

    return wrapper
