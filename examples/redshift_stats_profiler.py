from quollio_core.redshift_stats_profiler import execute
from quollio_core.repository.qdc import QDCExternalAPIClient
from quollio_core.repository.redshift import RedshiftConnectionConfig


def main():
    """
    You can create views for collecting stats in your redshift account.
    Data ingesting will be skipped, just creating views.
    """
    company_id = "<your company id>"
    build_view_connection = RedshiftConnectionConfig(
        host="<endpoint>",
        database="<database>",
        user="<username>",
        password="<password>",
        schema="(Optional)<schema> If you don't specify this attribute, the value will be public",
        port="(Optional)port If you don't specify this attribute, the value will be 5439",
        threads="(Optional)<threads> If you don't specify this attribute, the value will be 2",
    )
    qdc_client = QDCExternalAPIClient(
        client_id="<client id issued on QDC.>",
        client_secret="<client secret>",
        base_url="<base endpoint>",
    )
    query_view_connection = RedshiftConnectionConfig(
        host="<endpoint>",
        database="<database>",
        user="<username>",
        password="<password>",
        schema="(Optional)<schema> If you don't specify this attribute, the value will be public",
        port="(Optional)port If you don't specify this attribute, the value will be 5439",
        threads="(Optional)<threads> If you don't specify this attribute, the value will be 2",
    )
    is_view_build_only = True  # or False if you want to ingest data.
    target_tables = ["<tables for stats>"]
    execute(
        company_id=company_id,
        redshift_build_view_connections=build_view_connection,
        qdc_client=qdc_client,
        is_view_build_only=is_view_build_only,
        redshift_query_connections=query_view_connection,
        target_tables=target_tables,
    )
