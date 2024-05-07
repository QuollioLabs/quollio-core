import logging
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional

from databricks.sdk.core import Config, HeaderFactory, oauth_service_principal
from databricks.sql.client import Connection, connect

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")


@dataclass
class DatabricksConnectionConfig:
    host: str
    http_path: str
    client_id: str
    client_secret: str
    catalog: str
    schema: str

    def as_dict(self) -> Dict[str, str]:
        return asdict(self)


class DatabricksQueryExecutor:
    def __init__(self, config: DatabricksConnectionConfig) -> None:
        self.config = config
        self.conn = self.__initialize()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()

    def __initialize(self) -> Connection:
        conn = connect(
            server_hostname=self.config.host,
            http_path=self.config.http_path,
            credentials_provider=self.credential_provider,
        )
        return conn

    def get_query_results(self, query: str) -> List[Dict[str, str]]:
        results_asdict: List[Dict[str, str]] = []
        with self.conn.cursor() as cur:
            try:
                cur.execute(query)
                result: List[Dict[str, str]] = cur.fetchall()
            except Exception as e:
                logging.error(query, exc_info=True)
                logging.error("databricks get_query_results failed. %s", e)
                raise

            for row in result:
                results_asdict.append(row.asDict())
        return results_asdict

    def credential_provider(self) -> Optional[HeaderFactory]:
        config = Config(
            host=f"https://{self.config.host}", client_id=self.config.client_id, client_secret=self.config.client_secret
        )
        return oauth_service_principal(config)
