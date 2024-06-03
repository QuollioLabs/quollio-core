import logging
from typing import Any, Dict, List

from quollio_core.profilers.lineage import gen_table_lineage_payload, parse_bigquery_table_lineage
from quollio_core.repository import qdc
from quollio_core.repository.bigquery import BigQueryClient, GCPLineageClient, get_entitiy_reference, get_search_request

logger = logging.getLogger(__name__)


def bigquery_table_lineage(
    qdc_client: qdc.QDCExternalAPIClient,
    tenant_id: str,
    project_id: str,
    regions: list,
    org_id: str,
    credentials: Any,
):
    lineage_client = GCPLineageClient(credentials)
    bq_client = BigQueryClient(credentials)

    datasets = bq_client.list_datasets(project_id)
    all_tables = generate_table_list(datasets, bq_client)
    lineage_links = generate_lineage_links(all_tables, lineage_client, project_id, regions)
    lineage_links = parse_bigquery_table_lineage(lineage_links)

    update_table_lineage_inputs = gen_table_lineage_payload(tenant_id=tenant_id, endpoint=org_id, tables=lineage_links)

    req_count = 0
    for update_table_lineage_input in update_table_lineage_inputs:
        logger.info(
            "Generating table lineage. downstream: %s -> %s-> %s",
            update_table_lineage_input.downstream_database_name,
            update_table_lineage_input.downstream_schema_name,
            update_table_lineage_input.downstream_table_name,
        )
        status_code = qdc_client.update_lineage_by_id(
            global_id=update_table_lineage_input.downstream_global_id,
            payload=update_table_lineage_input.upstreams.as_dict(),
        )
        if status_code == 200:
            req_count += 1
    logger.info("Generating table lineage is finished. %s lineages are ingested.", req_count)


def generate_table_list(datasets: List[str], bq_client: BigQueryClient) -> List[str]:
    all_tables = []
    for dataset in datasets:
        all_tables.extend(
            [
                table
                for table in bq_client.list_tables(dataset.dataset_id)
                if table.table_type in ["TABLE", "VIEW", "MATERIALIZED_VIEW"]
            ]
        )

    all_table_names = []
    for table in all_tables:
        all_table_names.append(f"{table.project}.{table.dataset_id}.{table.table_id}")

    return all_table_names


def generate_lineage_links(
    all_tables: List[str], lineage_client: GCPLineageClient, project_id: str, regions: List[str]
) -> Dict[str, List[str]]:
    lineage_links = {}
    for table in all_tables:
        downstream = get_entitiy_reference()
        downstream.fully_qualified_name = f"bigquery:{table}"

        for region in regions:
            request = get_search_request(downstream_table=downstream, project_id=project_id, region=region)
            response = lineage_client.get_links(request=request)
            for lineage in response:
                target_table = str(lineage.target.fully_qualified_name).replace("bigquery:", "")
                if target_table not in lineage_links:
                    lineage_links[target_table] = []
                lineage_links[target_table].append(str(lineage.source.fully_qualified_name).replace("bigquery:", ""))

    return lineage_links
