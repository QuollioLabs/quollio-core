import logging


def set_log_level(level: str = "info") -> None:
    fmt = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    if level == "info":
        logging.basicConfig(level=logging.INFO, format=fmt)
    elif level == "debug":
        logging.basicConfig(level=logging.DEBUG, format=fmt)
    elif level == "warn":
        logging.basicConfig(level=logging.WARNING, format=fmt)
    elif level == "error":
        logging.basicConfig(level=logging.ERROR, format=fmt)
    elif level == "critical":
        logging.basicConfig(level=logging.CRITICAL, format=fmt)
    else:
        logging.basicConfig(level=logging.NOTSET, format=fmt)
