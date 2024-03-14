import functools
import logging
from typing import List, Optional, Set, Tuple

import sqlglot
from pydantic import BaseModel
from sqlglot import exp, optimizer
from sqlglot.errors import ParseError

from quollio_core.helper.core import new_global_id
from quollio_core.profilers.lineage import LineageInput, LineageInputs

logger = logging.getLogger(__name__)


@functools.total_ordering
class _FrozenModel(BaseModel, frozen=True):
    def __lt__(self, other: "_FrozenModel") -> bool:
        for field in self.__fields__:
            self_v = getattr(self, field)
            other_v = getattr(other, field)
            if self_v != other_v:
                return self_v < other_v

        return False


class Table(_FrozenModel):
    db: Optional[str] = None
    db_schema: Optional[str] = None
    table: str

    @classmethod
    def from_sqlglot_table(cls, table: exp.Table, db: str = None, schema: str = None) -> "Table":
        database = table.catalog
        db_schema = table.db
        if database == "":
            database = db or database
        if db_schema == "":
            db_schema = schema or db_schema
        return cls(db=database, db_schema=db_schema, table=table.this.name)


class SQLLineage:
    def __init__(self):
        self.dialects_use_uppercase_normally = ["oracle", "snowflake"]

    def get_table_level_lineage_source(
        self,
        sql: str,
        dialect: str,
        src_db: str = None,
        src_schema: str = None,
        dest_db: str = None,
        dest_schema: str = None,
    ) -> Tuple[Set[Table], Table]:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
        try:
            statement: sqlglot.Expression = sqlglot.parse_one(sql=sql, error_level=sqlglot.ErrorLevel.RAISE)
        except ParseError as e:
            logger.error("SQL parse error.\nSQL statement:{sql} \nError:{err}\n".format(sql=sql, err=e))
            raise

        if dialect in self.dialects_use_uppercase_normally:
            src_db = src_db.upper() if src_db is not None else None
            src_schema = src_schema.upper() if src_schema is not None else None
            dest_db = dest_db.upper() if dest_db is not None else None
            dest_schema = dest_schema.upper() if dest_schema is not None else None

        # MEMO: Complement sql with dialect, source database and source schema info.
        optimized_stmt: sqlglot.Expression = optimizer.qualify.qualify(
            statement,
            dialect=dialect,
            catalog=src_db,
            db=src_schema,
            qualify_columns=False,
            validate_qualify_columns=False,
            identify=False,
        )

        orig_dest_table = Table(table="")
        dest_table = Table(table="")
        for expr in optimized_stmt.find_all(exp.Create, exp.Insert, exp.Update, exp.Delete, exp.Merge):
            if isinstance(expr.this, exp.Table):
                orig_dest_table = Table.from_sqlglot_table(
                    table=expr.this
                )  # MEMO: use this to remove duplication from source tables.
                dest_table = Table.from_sqlglot_table(table=expr.this, db=dest_db, schema=dest_schema)

        cte_tables: Set[Table] = set()
        for cte in optimized_stmt.find_all(exp.CTE):
            cte_table = Table(db=None, schema=None, table=cte.alias_or_name)
            cte_tables.add(cte_table)

        source_tables: Set[Table] = set()
        for expr in optimized_stmt.find_all(exp.Table):
            src_orig_table = Table.from_sqlglot_table(expr)
            if src_orig_table != orig_dest_table and src_orig_table not in cte_tables:
                src_table = Table.from_sqlglot_table(expr, db=src_db, schema=src_schema)
                source_tables.add(src_table)

        return source_tables, dest_table

    def gen_lineage_input(
        self, tenant_id: str, endpoint: str, src_tables: Set[Table], dest_table: Table
    ) -> List[LineageInputs]:
        lineage_input = LineageInput(upstream=[])
        downstream_global_id_arg = "{db}{schema}{table}".format(
            db=dest_table.db, schema=dest_table.db_schema, table=dest_table.table
        )
        downstream_global_id = new_global_id(
            tenant_id=tenant_id, cluster_id=endpoint, data_id=downstream_global_id_arg, data_type="table"
        )

        for src_table in src_tables:
            upstream_global_id_arg = "{db}{schema}{table}".format(
                db=src_table.db, schema=src_table.db_schema, table=src_table.table
            )
            upstream_table_global_id = new_global_id(
                tenant_id=tenant_id, cluster_id=endpoint, data_id=upstream_global_id_arg, data_type="table"
            )
            lineage_input.upstream.append(upstream_table_global_id)

        lineage_inputs = LineageInputs(
            downstream_global_id=downstream_global_id,
            downstream_database_name=dest_table.db,
            downstream_schema_name=dest_table.db_schema,
            downstream_table_name=dest_table.table,
            downstream_column_name="",
            upstreams=lineage_input,
        )
        return lineage_inputs
