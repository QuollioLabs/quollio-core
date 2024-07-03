import logging
from typing import List

from quollio_core.profilers.lineage import gen_table_lineage_payload, gen_table_lineage_payload_inputs
from quollio_core.profilers.sqllineage import SQLLineage
from quollio_core.profilers.stats import (
    gen_table_stats_payload_from_tuple,
    get_is_target_stats_items,
    render_sql_for_stats,
)
from quollio_core.repository import qdc, redshift

logger = logging.getLogger(__name__)


def redshift_table_level_lineage(
    conn: redshift.RedshiftConnectionConfig,
    qdc_client: qdc.QDCExternalAPIClient,
    tenant_id: str,
    dbt_table_name: str,
) -> None:
    with redshift.RedshiftQueryExecutor(config=conn) as redshift_executor:
        results = redshift_executor.get_query_results(
            query="""
            SELECT
                *
            FROM
                {db}.{schema}.{table}
            """.format(
                db=conn.database,
                schema=conn.schema,
                table=dbt_table_name,
            )
        )
        lineage_payload_inputs = gen_table_lineage_payload_inputs(input_data=results)

        update_table_lineage_inputs = gen_table_lineage_payload(
            tenant_id=tenant_id,
            endpoint=conn.host,
            tables=lineage_payload_inputs,
        )

        req_count = 0
        for update_table_lineage_input in update_table_lineage_inputs:
            logger.info(
                "Generating table lineage. downstream: {db} -> {schema} -> {table}".format(
                    db=update_table_lineage_input.downstream_database_name,
                    schema=update_table_lineage_input.downstream_schema_name,
                    table=update_table_lineage_input.downstream_table_name,
                )
            )
            status_code = qdc_client.update_lineage_by_id(
                global_id=update_table_lineage_input.downstream_global_id,
                payload=update_table_lineage_input.upstreams.as_dict(),
            )
            if status_code == 200:
                req_count += 1
        logger.info(f"Generating table lineage is finished. {req_count} lineages are ingested.")
    return


def _gen_get_stats_views_query(db: str, schema: str) -> str:
    query = """
        SELECT
            DISTINCT
            table_catalog
            , table_schema
            , table_name
        FROM
            {db}.INFORMATION_SCHEMA.TABLES
        WHERE
            table_name LIKE 'quollio_stats_columns_%%'
            AND table_schema = '{schema}'
        """.format(
        db=db, schema=schema
    )
    return query


def redshift_table_stats(
    conn: redshift.RedshiftConnectionConfig,
    qdc_client: qdc.QDCExternalAPIClient,
    tenant_id: str,
    stats_items: List[str],
) -> None:
    is_aggregate_items = get_is_target_stats_items(stats_items=stats_items)
    with redshift.RedshiftQueryExecutor(config=conn) as redshift_executor:
        stats_query = _gen_get_stats_views_query(
            db=conn.database,
            schema=conn.schema,
        )
        stats_views = redshift_executor.get_query_results(query=stats_query)
        logger.info("Found %s for table statistics.", len(stats_views))

        req_count = 0
        for stats_view in stats_views:
            table_fqn = "{catalog}.{schema}.{table}".format(
                catalog=stats_view[0], schema=stats_view[1], table=stats_view[2]
            )
            stats_query = render_sql_for_stats(is_aggregate_items=is_aggregate_items, table_fqn=table_fqn)
            logger.debug(f"The following sql will be fetched to retrieve stats values. {stats_query}")
            stats_result = redshift_executor.get_query_results(query=stats_query)
            payloads = gen_table_stats_payload_from_tuple(tenant_id=tenant_id, endpoint=conn.host, stats=stats_result)
            for payload in payloads:
                logger.info(
                    "Generating table stats. asset: {db} -> {schema} -> {table} -> {column}".format(
                        db=payload.db,
                        schema=payload.schema,
                        table=payload.table,
                        column=payload.column,
                    )
                )
                status_code = qdc_client.update_stats_by_id(
                    global_id=payload.global_id,
                    payload=payload.body.get_column_stats(),
                )
                if status_code == 200:
                    req_count += 1
    logger.info(f"Generating table stats is finished. {req_count} stats are ingested.")
    return


def redshift_table_level_sqllineage(
    conn: redshift.RedshiftConnectionConfig,
    qdc_client: qdc.QDCExternalAPIClient,
    tenant_id: str,
) -> None:
    redshift_connector = redshift.RedshiftQueryExecutor(conn)
    results = redshift_connector.get_query_results(
        query="""
        SELECT
            database_name
            , schema_name
            , query_text
        FROM
            {db}.{schema}.QUOLLIO_SQLLINEAGE_SOURCES
        """.format(
            db=conn.database,
            schema=conn.schema,
        )
    )
    update_table_lineage_inputs_list = list()
    sql_lineage = SQLLineage()
    for result in results:
        src_tables, dest_table = sql_lineage.get_table_level_lineage_source(
            sql=result[2],
            dialect="redshift",
            dest_db=result[0],
            dest_schema=result[1],
        )
        update_table_lineage_inputs = sql_lineage.gen_lineage_input(
            tenant_id=tenant_id, endpoint=conn.host, src_tables=src_tables, dest_table=dest_table
        )
        update_table_lineage_inputs_list.append(update_table_lineage_inputs)

    req_count = 0
    for update_table_lineage_input in update_table_lineage_inputs_list:
        logger.info(
            "Generating table lineage. downstream: {db} -> {schema} -> {table}".format(
                db=update_table_lineage_input.downstream_database_name,
                schema=update_table_lineage_input.downstream_schema_name,
                table=update_table_lineage_input.downstream_table_name,
            )
        )
        status_code = qdc_client.update_lineage_by_id(
            global_id=update_table_lineage_input.downstream_global_id,
            payload=update_table_lineage_input.upstreams.as_dict(),
        )
        if status_code == 200:
            req_count += 1
    logger.info(f"Generating table lineage is finished. {req_count} lineages are ingested.")
    return
