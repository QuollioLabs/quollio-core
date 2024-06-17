import logging
from typing import List

from dbt.cli.main import dbtRunner, dbtRunnerResult

logger = logging.getLogger(__name__)


class DBTClient:
    def __init__(self) -> None:
        self.dbt = dbtRunner()

    def invoke(self, cmd: str, project_dir: str, profile_dir: str, options: List[str] = None) -> dbtRunnerResult:
        req = [cmd, "--project-dir", project_dir, "--profiles-dir", profile_dir]
        if options is not None:
            req.extend(options)
        res = self.dbt.invoke(req)

        if not res.success:
            logger.error(f"dbt.invoke failed. cmd: {cmd}", res)
            raise Exception(res.result)
        return res
