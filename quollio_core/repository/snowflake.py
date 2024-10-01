import logging
from dataclasses import asdict, dataclass
from typing import Dict, List, Tuple

from snowflake.connector import DictCursor, connect, errors
from snowflake.connector.connection import SnowflakeConnection

logger = logging.getLogger(__name__)


@dataclass
class SnowflakeConnectionConfig:
    account_id: str
    account_user: str
    account_password: str
    account_build_role: str
    account_query_role: str
    account_warehouse: str
    account_database: str
    account_schema: str
    threads: int = 3

    def as_dict(self) -> Dict[str, str]:
        return asdict(self)


class SnowflakeQueryExecutor:
    def __init__(self, config: SnowflakeConnectionConfig) -> None:
        self.conn = self.__initialize(config)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()

    def __initialize(self, config: SnowflakeConnectionConfig) -> SnowflakeConnection:
        conn: SnowflakeConnection = connect(
            user=config.account_user,
            password=config.account_password,
            role=config.account_query_role,
            account=config.account_id,
            warehouse=config.account_warehouse,
            database=config.account_database,
            schema=config.account_schema,
        )
        return conn

    def get_query_results(self, query: str) -> Tuple[List[Dict[str, str]], Exception]:
        with self.conn.cursor(DictCursor) as cur:
            try:
                cur.execute(query)
                result: List[Dict[str, str]] = cur.fetchall()
                return (result, None)
            except errors.ProgrammingError as e:
                return ([], e)
            except Exception as e:
                return ([], e)
