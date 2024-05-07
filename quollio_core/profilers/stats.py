import logging
from dataclasses import asdict, dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import Dict, List, Tuple, Union

from quollio_core.helper.core import new_global_id

logger = logging.getLogger(__name__)


@dataclass
class ColumnStatsInput:
    cardinality: int
    max: str
    mean: str
    median: str
    min: str
    mode: str
    number_of_null: int
    number_of_unique: int
    stddev: str

    def as_dict(self) -> Dict[str, str]:
        return asdict(self)


@dataclass
class TableStatsInput:
    count: int
    size: float

    def as_dict(self) -> Dict[str, str]:
        return asdict(self)


@dataclass
class StatsInput:
    column_stats: ColumnStatsInput
    table_stats: TableStatsInput

    def as_dict(self) -> Dict[str, str]:
        return asdict(self)

    def get_column_stats(self):
        return {"column_stats": self.column_stats.as_dict()}


@dataclass
class StatsRequest:
    global_id: str
    db: str
    schema: str
    table: str
    column: str
    body: StatsInput

    def as_dict(self) -> Dict[str, str]:
        return asdict(self)


def convert_value_type(obj, cast_str: bool = False):
    if obj is None:
        return None
    if isinstance(obj, Decimal):
        return str(obj.quantize(Decimal("0.1"), ROUND_HALF_UP))
    if cast_str:
        return str(obj)
    return obj


def gen_table_stats_payload(tenant_id: str, endpoint: str, stats: List[Dict[str, str]]) -> List[StatsRequest]:
    payloads = list()
    for stat in stats:
        global_id_arg = "{db}{schema}{table}{column}".format(
            db=stat["DB_NAME"], schema=stat["SCHEMA_NAME"], table=stat["TABLE_NAME"], column=stat["COLUMN_NAME"]
        )
        table_global_id = new_global_id(
            tenant_id=tenant_id, cluster_id=endpoint, data_id=global_id_arg, data_type="column"
        )
        stats_request = StatsRequest(
            global_id=table_global_id,
            db=stat["DB_NAME"],
            schema=stat["SCHEMA_NAME"],
            table=stat["TABLE_NAME"],
            column=stat["COLUMN_NAME"],
            body=StatsInput(
                column_stats=ColumnStatsInput(
                    cardinality=convert_value_type(stat["CARDINALITY"]),
                    max=convert_value_type(stat["MAX_VALUE"], True),
                    mean=convert_value_type(stat["AVG_VALUE"], True),
                    median=convert_value_type(stat["MEDIAN_VALUE"], True),
                    min=convert_value_type(stat["MIN_VALUE"], True),
                    mode=convert_value_type(stat["MODE_VALUE"], True),
                    number_of_null=convert_value_type(stat["NULL_COUNT"]),
                    number_of_unique=convert_value_type(stat["CARDINALITY"]),
                    stddev=convert_value_type(stat["STDDEV_VALUE"], True),
                ),
                # MEMO: Table stats can be collected with metadata agent.
                # Then, It's not necessary to update with this system for now.
                table_stats=TableStatsInput(count=0, size=0.0),
            ),
        )
        payloads.append(stats_request)
    return payloads


def gen_table_stats_payload_from_tuple(
    tenant_id: str, endpoint: str, stats: Tuple[List[str]]
) -> List[Dict[str, Union[str, List[Dict[str, str]]]]]:
    payloads = list()
    for stat in stats:
        global_id_arg = "{db}{schema}{table}{column}".format(db=stat[0], schema=stat[1], table=stat[2], column=stat[3])
        table_global_id = new_global_id(
            tenant_id=tenant_id, cluster_id=endpoint, data_id=global_id_arg, data_type="column"
        )
        stats_request = StatsRequest(
            global_id=table_global_id,
            db=stat[0],
            schema=stat[1],
            table=stat[2],
            column=stat[3],
            body=StatsInput(
                column_stats=ColumnStatsInput(
                    max=convert_value_type(stat[4], True),
                    min=convert_value_type(stat[5], True),
                    number_of_null=convert_value_type(stat[6]),
                    cardinality=convert_value_type(stat[7]),
                    mean=convert_value_type(stat[8], True),
                    median=convert_value_type(stat[9], True),
                    mode=convert_value_type(stat[10], True),
                    number_of_unique=convert_value_type(stat[7]),
                    stddev=convert_value_type(stat[11], True),
                ),
                # MEMO: Table stats can be collected with metadata agent.
                # Then, It's not necessary to update with this system for now.
                table_stats=TableStatsInput(count=0, size=0.0),
            ),
        )
        payloads.append(stats_request)
    return payloads
