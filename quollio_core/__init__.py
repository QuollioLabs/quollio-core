"""Quollio Data Profiler"""

__version__ = "0.2.1"
__author__ = "Quollio technologies"


from quollio_core.repository.snowflake import SnowflakeConnectionConfig  # noqa:F401
from quollio_core.snowflake_lineage_profiler import execute  # noqa:F401
