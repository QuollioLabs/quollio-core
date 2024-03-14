import logging
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Dict, List, Tuple

from redshift_connector import Connection, connect
from redshift_connector.error import ProgrammingError

logger = logging.getLogger(__name__)


# MEMO: https://docs.aws.amazon.com/redshift/latest/mgmt/rsql-query-tool-error-codes.html
class RedshiftErrorCode(Enum):
    # Connection
    CONNECTION_EXCEPTION = "08000"
    CONNECTION_DOES_NOT_EXIST = "08003"
    CONNECTION_FAILURE = "08006"
    SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION = "08001"
    SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION = "08004"
    TRANSACTION_RESOLUTION_UNKNOWN = "08007"
    PROTOCOL_VIOLATION = "08P01"

    # Privilege
    INSUFFICIENT_PRIVILEGE = "42501"

    # Resource
    OUT_OF_MEMORY = "53200"

    @classmethod
    def match_error_code(cls, error_code: str) -> bool:
        for code in cls:
            if code.value == error_code:
                return True
        return False


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
                err_body = dict()
                if 1 <= len(pe.args):
                    err_body = pe.args[0]

                error_code = err_body.get("C")
                if RedshiftErrorCode.match_error_code(error_code):
                    err = RedshiftErrorCode(error_code)
                    logger.error(" ".join(query.split()))
                    logger.error("{err} error happened: {body}".format(err=err.name, body=pe))
                else:
                    logger.error(query)
                    logger.error("ProgrammingError: {err}".format(err=pe))
                    raise
            except Exception as e:
                logger.error(query)
                logger.error("Failed to get query results. error: {err}".format(err=e))
                raise
