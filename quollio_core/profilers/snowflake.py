import logging

from quollio_core.profilers.lineage import (
    gen_column_lineage_payload,
    gen_table_lineage_payload,
    parse_snowflake_results,
)
from quollio_core.profilers.sqllineage import SQLLineage
from quollio_core.profilers.stats import gen_table_stats_payload
from quollio_core.repository import qdc, snowflake

logger = logging.getLogger(__name__)


def snowflake_table_to_table_lineage(
    conn: snowflake.SnowflakeConnectionConfig,
    qdc_client: qdc.QDCExternalAPIClient,
    tenant_id: str,
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    sf_executor = snowflake.SnowflakeQueryExecutor(conn)
    results = sf_executor.get_query_results(
        query="""
        SELECT
            *
        FROM
           {db}.{schema}.QUOLLIO_LINEAGE_TABLE_LEVEL
        """.format(
            db=conn.account_database,
            schema=conn.account_schema,
        )
    )
    parsed_results = parse_snowflake_results(results=results)
    update_table_lineage_inputs = gen_table_lineage_payload(
        tenant_id=tenant_id,
        endpoint=conn.account_id,
        tables=parsed_results,
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


def snowflake_column_to_column_lineage(
    conn: snowflake.SnowflakeConnectionConfig,
    qdc_client: qdc.QDCExternalAPIClient,
    tenant_id: str,
) -> None:
    sf_executor = snowflake.SnowflakeQueryExecutor(conn)
    results = sf_executor.get_query_results(
        query="""
        SELECT
            *
        FROM
            {db}.{schema}.QUOLLIO_LINEAGE_COLUMN_LEVEL
        """.format(
            db=conn.account_database,
            schema=conn.account_schema,
        )
    )
    update_column_lineage_inputs = gen_column_lineage_payload(
        tenant_id=tenant_id,
        endpoint=conn.account_id,
        columns=results,
    )

    req_count = 0
    for update_column_lineage_input in update_column_lineage_inputs:
        logger.info(
            "Generating column lineage. downstream: {db} -> {schema} -> {table} -> {column}".format(
                db=update_column_lineage_input.downstream_database_name,
                schema=update_column_lineage_input.downstream_schema_name,
                table=update_column_lineage_input.downstream_table_name,
                column=update_column_lineage_input.downstream_column_name,
            )
        )
        status_code = qdc_client.update_lineage_by_id(
            global_id=update_column_lineage_input.downstream_global_id,
            payload=update_column_lineage_input.upstreams.as_dict(),
        )
        if status_code == 200:
            req_count += 1
    logger.info(f"Generating column lineage is finished. {req_count} lineages are ingested.")
    return


def snowflake_table_level_sqllineage(
    conn: snowflake.SnowflakeConnectionConfig,
    qdc_client: qdc.QDCExternalAPIClient,
    tenant_id: str,
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    sf_executor = snowflake.SnowflakeQueryExecutor(conn)
    results = sf_executor.get_query_results(
        query="""
        SELECT
            database_name
            , schema_name
            , query_text
        FROM
           {db}.{schema}.QUOLLIO_SQLLINEAGE_SOURCES
        """.format(
            db=conn.account_database,
            schema=conn.account_schema,
        )
    )
    update_table_lineage_inputs_list = list()
    sql_lineage = SQLLineage()
    for result in results:
        src_tables, dest_table = sql_lineage.get_table_level_lineage_source(
            sql=result["QUERY_TEXT"],
            dialect="snowflake",
            dest_db=result["DATABASE_NAME"],
            dest_schema=result["SCHEMA_NAME"],
        )
        update_table_lineage_inputs = sql_lineage.gen_lineage_input(
            tenant_id=tenant_id, endpoint=conn.account_id, src_tables=src_tables, dest_table=dest_table
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


def _get_target_tables_query(db: str, schema: str) -> str:
    query = """
        SELECT
            DISTINCT
            TABLE_CATALOG
            , TABLE_SCHEMA
            , TABLE_NAME
        FROM
            {db}.{schema}.QUOLLIO_STATS_PROFILING_COLUMNS
        """.format(
        db=db, schema=schema
    )
    return query


def _get_stats_tables_query(db: str, schema: str) -> str:
    query = """
        SELECT
            DISTINCT
            TABLE_CATALOG
            , TABLE_SCHEMA
            , TABLE_NAME
        FROM
            {db}.INFORMATION_SCHEMA.TABLES
        WHERE
            startswith(TABLE_NAME, 'QUOLLIO_STATS_COLUMNS_')
            AND TABLE_SCHEMA = UPPER('{schema}')
        """.format(
        db=db, schema=schema
    )
    return query


def snowflake_table_stats(
    conn: snowflake.SnowflakeConnectionConfig,
    qdc_client: qdc.QDCExternalAPIClient,
    tenant_id: str,
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    sf_executor = snowflake.SnowflakeQueryExecutor(conn)

    target_query = _get_target_tables_query(
        db=conn.account_database,
        schema=conn.account_schema,
    )
    target_assets = sf_executor.get_query_results(query=target_query)

    stats_query = _get_stats_tables_query(
        db=conn.account_database,
        schema=conn.account_schema,
    )
    stats_columns = sf_executor.get_query_results(query=stats_query)

    req_count = 0
    for target_asset in target_assets:
        for stats_column in stats_columns:
            stats_query = """
            SELECT
                db_name
                , schema_name
                , table_name
                , column_name
                , max_value
                , min_value
                , null_count
                , cardinality
                , avg_value
                , median_value
                , mode_value
                , stddev_value
            FROM
                {db}.{schema}.{table}
            WHERE
                db_name = '{target_db}'
                and schema_name = '{target_schema}'
                and table_name = '{target_table}'
            """.format(
                db=stats_column["TABLE_CATALOG"],
                schema=stats_column["TABLE_SCHEMA"],
                table=stats_column["TABLE_NAME"],
                target_db=target_asset["TABLE_CATALOG"],
                target_schema=target_asset["TABLE_SCHEMA"],
                target_table=target_asset["TABLE_NAME"],
            )
            stats_result = sf_executor.get_query_results(query=stats_query)
            payloads = gen_table_stats_payload(tenant_id=tenant_id, endpoint=conn.account_id, stats=stats_result)
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
