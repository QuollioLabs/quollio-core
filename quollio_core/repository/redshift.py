import logging
from dataclasses import asdict, dataclass
from typing import Dict, List, Tuple

from redshift_connector import Connection, connect

logger = logging.getLogger(__name__)


@dataclass
class RedshiftConnectionConfig:
    host: str
    database: str
    user: str
    password: str
    schema: str = "public"
    port: int = 5439
    threads: int = 2

    def as_dict(self) -> Dict[str, str]:
        return asdict(self)


class RedshiftQueryExecutor:
    def __init__(self, config: RedshiftConnectionConfig):
        self.conn = self.__initialize(config)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()

    def __initialize(self, config: RedshiftConnectionConfig) -> Connection:
        conn: RedshiftConnectionConfig = connect(
            host=config.host, database=config.database, user=config.user, password=config.password
        )
        return conn

    def get_query_results(self, query: str) -> Tuple[List[str]]:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
        with self.conn.cursor() as cur:
            try:
                cur.execute(query)
                result: tuple = cur.fetchall()
                return result
            except Exception as e:
                logger.error("Failed to get query results. error: {err}".format(err=e))
                raise
