import logging
from dataclasses import asdict, dataclass
from typing import Dict, List

from snowflake.connector import DictCursor, connect, errors
from snowflake.connector.connection import SnowflakeConnection

logger = logging.getLogger(__name__)


@dataclass
class SnowflakeConnectionConfig:
    account_id: str
    account_role: str
    account_user: str
    account_password: str
    account_warehouse: str

    def as_dict(self) -> Dict[str, str]:
        return asdict(self)


class SnowflakeQueryExecutor:
    def __init__(self, config: SnowflakeConnectionConfig, db: str, schema: str) -> None:
        self.conn = self.__initialize(config, db, schema)

    def __initialize(self, config: SnowflakeConnectionConfig, db: str, schema: str) -> SnowflakeConnection:
        conn: SnowflakeConnection = connect(
            user=config.account_user,
            password=config.account_password,
            role=config.account_role,
            account=config.account_id,
            warehouse=config.account_warehouse,
            database=db,
            schema=schema,
        )
        return conn

    def get_query_results(self, query: str) -> List[Dict[str, str]]:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
        with self.conn.cursor(DictCursor) as cur:
            try:
                cur.execute(query)
                result: List[Dict[str, str]] = cur.fetchall()
                return result
            except errors.ProgrammingError as e:
                logger.error(
                    "snowflake get_query_results failed. {0} ({1}): {2} ({3})".format(
                        e.errno, e.sqlstate, e.msg, e.sfqid
                    )
                )