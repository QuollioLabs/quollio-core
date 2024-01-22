import logging
from dataclasses import asdict, dataclass
from typing import Dict, List, Tuple

from redshift_connector import Connection, connect
from redshift_connector.error import ProgrammingError

logger = logging.getLogger(__name__)


@dataclass
class RedshiftConnectionConfig:
    host: str
    build_user: str
    query_user: str
    build_password: str
    query_password: str
    database: str
    schema: str
    port: int = 5439
    threads: int = 3

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
            host=config.host, database=config.database, user=config.query_user, password=config.query_password
        )
        return conn

    def get_query_results(self, query: str) -> Tuple[List[str]]:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
        with self.conn.cursor() as cur:
            try:
                cur.execute(query)
                result: tuple = cur.fetchall()
                return result
            except ProgrammingError as pe:
                if "out of memory" in str(pe).lower():
                    logger.error(" ".join(query.split()))
                    logger.error("Out of Memory Error: {err}".format(err=pe))
                    return tuple()
                else:
                    logger.error(query)
                    logger.error("ProgrammingError: {err}".format(err=pe))
                    raise
            except Exception as e:
                logger.error(query)
                logger.error("Failed to get query results. error: {err}".format(err=e))
                raise
