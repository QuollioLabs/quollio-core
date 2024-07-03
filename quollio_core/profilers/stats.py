import logging
from dataclasses import asdict, dataclass, fields
from decimal import ROUND_HALF_UP, Decimal
from typing import Dict, List, Tuple, Union

from jinja2 import Template

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
        db_name = stat.get("DB_NAME", stat.get("db_name"))
        schema_name = stat.get("SCHEMA_NAME", stat.get("schema_name"))
        table_name = stat.get("TABLE_NAME", stat.get("table_name"))
        column_name = stat.get("COLUMN_NAME", stat.get("column_name"))
        global_id_arg = "{db}{schema}{table}{column}".format(
            db=db_name, schema=schema_name, table=table_name, column=column_name
        )
        table_global_id = new_global_id(
            tenant_id=tenant_id, cluster_id=endpoint, data_id=global_id_arg, data_type="column"
        )
        column_stats_input = ColumnStatsInput(
            cardinality=convert_value_type(stat.get("CARDINALITY", stat.get("cardinality"))),
            max=convert_value_type(stat.get("MAX_VALUE", stat.get("max_value")), True),
            mean=convert_value_type(stat.get("AVG_VALUE", stat.get("avg_value")), True),
            median=convert_value_type(stat.get("MEDIAN_VALUE", stat.get("median_value")), True),
            min=convert_value_type(stat.get("MIN_VALUE", stat.get("min_value")), True),
            mode=convert_value_type(stat.get("MODE_VALUE", stat.get("mode_value")), True),
            number_of_null=convert_value_type(stat.get("NULL_COUNT", stat.get("null_count"))),
            number_of_unique=convert_value_type(stat.get("CARDINALITY", stat.get("cardinality"))),
            stddev=convert_value_type(stat.get("STDDEV_VALUE", stat.get("stddev_value")), True),
        )
        stats_request = StatsRequest(
            global_id=table_global_id,
            db=db_name,
            schema=schema_name,
            table=table_name,
            column=column_name,
            body=StatsInput(
                column_stats=column_stats_input,
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


def render_sql_for_stats(is_aggregate_items: Dict[str, bool], table_fqn: str, cte: str = "") -> str:
    sql_template_for_stats = Template(
        """
        {% if cte -%}
          {{ cte }}
        {% endif -%}
        SELECT
            db_name
            , schema_name
            , table_name
            , column_name
            , {% if agg_max == True -%} max_value {% else -%} null as max_value {% endif %}
            , {% if agg_min == True -%} min_value {% else -%} null as min_value {% endif %}
            , {% if agg_null_count == True -%} null_count {% else -%} null as null_count {% endif %}
            , {% if agg_cardinality == True -%} cardinality {% else -%} null as cardinality {% endif %}
            , {% if agg_avg == True -%} avg_value {% else -%} null as avg_value {% endif %}
            , {% if agg_median == True -%} median_value {% else -%} null as median_value {% endif %}
            , {% if agg_mode == True -%} mode_value {% else -%} null as mode_value {% endif %}
            , {% if agg_stddev == True -%} stddev_value {% else -%} null as stddev_value {% endif %}
        FROM
            {{ table_fqn }}
    """
    )
    query = sql_template_for_stats.render(
        agg_max=is_aggregate_items["max"],
        agg_min=is_aggregate_items["min"],
        agg_null_count=is_aggregate_items["number_of_null"],
        agg_cardinality=is_aggregate_items["cardinality"],
        agg_avg=is_aggregate_items["mean"],
        agg_median=is_aggregate_items["median"],
        agg_mode=is_aggregate_items["mode"],
        agg_stddev=is_aggregate_items["stddev"],
        table_fqn=table_fqn,
        cte=cte,
    )
    return query


def get_is_target_stats_items(stats_items: List[str]) -> List[Dict[str, bool]]:
    target_stats_fields = get_column_stats_items()
    is_aggregate_items = dict()
    for target_stats_field in target_stats_fields:
        is_aggregate_items[target_stats_field] = False

    for stats_item in stats_items:
        is_aggregate_items[stats_item] = True

    return is_aggregate_items


def get_column_stats_items() -> List[str]:
    return [field.name for field in fields(ColumnStatsInput)]
