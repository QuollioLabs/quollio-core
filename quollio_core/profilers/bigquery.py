from typing import Dict, List

from google.auth.credentials import Credentials

from quollio_core.helper.log_utils import error_handling_decorator, logger
from quollio_core.profilers.lineage import gen_table_lineage_payload, parse_bigquery_table_lineage
from quollio_core.profilers.stats import gen_table_stats_payload
from quollio_core.repository import qdc
from quollio_core.repository.bigquery import BigQueryClient, GCPLineageClient, get_entitiy_reference, get_search_request


@error_handling_decorator
def bigquery_table_lineage(
    qdc_client: qdc.QDCExternalAPIClient,
    tenant_id: str,
    project_id: str,
    regions: list,
    org_id: str,
    credentials: Credentials,
) -> None:
    lineage_client = GCPLineageClient(credentials)
    bq_client = BigQueryClient(credentials, project_id)

    datasets = bq_client.list_dataset_ids()
    all_tables = generate_table_list(bq_client, datasets)
    lineage_links = generate_lineage_links(all_tables, lineage_client, project_id, regions)
    lineage_links = parse_bigquery_table_lineage(lineage_links)
    logger.debug("The following resources will be ingested. %s", lineage_links)

    update_table_lineage_inputs = gen_table_lineage_payload(tenant_id=tenant_id, endpoint=org_id, tables=lineage_links)

    req_count = 0
    for update_table_lineage_input in update_table_lineage_inputs:
        logger.info(
            "Generating table lineage. downstream: %s -> %s -> %s. upstream: %s",
            update_table_lineage_input.downstream_database_name,
            update_table_lineage_input.downstream_schema_name,
            update_table_lineage_input.downstream_table_name,
            update_table_lineage_input.upstreams.as_dict(),
        )
        status_code = qdc_client.update_lineage_by_id(
            global_id=update_table_lineage_input.downstream_global_id,
            payload=update_table_lineage_input.upstreams.as_dict(),
        )
        if status_code == 200:
            req_count += 1
    logger.info("Generating table lineage is finished. %s lineages are ingested.", req_count)


@error_handling_decorator
def bigquery_table_stats(
    qdc_client: qdc.QDCExternalAPIClient,
    bq_client: BigQueryClient,
    tenant_id: str,
    org_id: str,
    dataplex_stats_tables: list,
) -> None:
    profiling_results = []
    for table in dataplex_stats_tables:
        logger.info("Profiling columns using Dataplex stats table: %s", table)
        profiling_results.extend(column_stats_from_dataplex(bq_client, table))

    stats = gen_table_stats_payload(tenant_id, org_id, profiling_results)

    for stat in stats:
        status_code = qdc_client.update_stats_by_id(
            global_id=stat.global_id,
            payload=stat.body.as_dict(),
        )
        if status_code == 200:
            logger.info(
                "Stats for column %s -> %s -> %s -> %s is successfully ingested.",
                stat.db,
                stat.schema,
                stat.table,
                stat.column,
            )
            logger.debug("Stats for column id %s is successfully ingested.", stat.global_id)


def generate_table_list(bq_client: BigQueryClient, datasets: List[str]) -> List[str]:
    all_tables = []
    for dataset in datasets:
        all_tables.extend(
            [
                table
                for table in bq_client.list_tables(dataset)
                if table["table_type"] in ["TABLE", "VIEW", "MATERIALIZED_VIEW"]
            ],
        )

    all_table_names = []
    for table in all_tables:
        all_table_names.append(f"{bq_client.client.project}.{table['dataset_id']}.{table['table_id']}")

    return all_table_names


def generate_lineage_links(
    all_tables: List[str],
    lineage_client: GCPLineageClient,
    project_id: str,
    regions: List[str],
) -> Dict[str, List[str]]:
    lineage_links = {}
    for table in all_tables:
        if "quollio" in table.lower():
            continue
        downstream = get_entitiy_reference()
        downstream.fully_qualified_name = f"bigquery:{table}"

        for region in regions:
            request = get_search_request(downstream_table=downstream, project_id=project_id, region=region)
            response = lineage_client.get_links(request=request)
            for lineage in response:
                target_table = str(lineage.target.fully_qualified_name).replace("bigquery:", "")
                source_table = str(lineage.source.fully_qualified_name).replace("bigquery:", "")
                if target_table not in lineage_links:
                    lineage_links[target_table] = []
                if source_table not in lineage_links[target_table]:
                    lineage_links[target_table].append(source_table)

    return lineage_links


def column_stats_from_dataplex(bq_client: BigQueryClient, profiling_table: str) -> List[Dict]:
    query = f"""
    SELECT
        data_source.table_project_id AS DB_NAME,
        data_source.dataset_id AS SCHEMA_NAME,
        data_source.table_id AS TABLE_NAME,
        column_name AS COLUMN_NAME,
        min_value AS MIN_VALUE,
        max_value AS MAX_VALUE,
        average_value AS AVG_VALUE,
        quartile_median AS MEDIAN_VALUE,
        standard_deviation AS STDDEV_VALUE,
        top_n[0][0] AS MODE_VALUE,
        CAST((percent_null / 100) * job_rows_scanned AS INT) as NULL_COUNT,
        CAST((percent_unique / 100) * job_rows_scanned AS INT) as CARDINALITY
    FROM `{profiling_table}`
    """
    logger.debug(f"Executing Query: {query}")
    results = bq_client.client.query(query).result()

    # Convert RowIterator to a list of dictionaries
    return [dict(row) for row in results]
