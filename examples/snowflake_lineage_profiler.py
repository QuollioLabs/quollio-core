from quollio_core.repository.qdc import QDCExternalAPIClient
from quollio_core.repository.snowflake import SnowflakeConnectionConfig
from quollio_core.snowflake_lineage_profiler import execute


def main():
    """
    You can create views for collecting lineage in your snowflake account.
    Data ingesting will be skipped, just creating views.
    """
    company_id = "<your company id>"
    build_view_connection = SnowflakeConnectionConfig(
        account_id="<snowflake account id. Please use the same account id of metadata agent.>",
        account_role="<account role for build view>",
        account_user="<username>",
        account_password="<password>",
        account_warehouse="<warehouse>",
    )
    qdc_client = QDCExternalAPIClient(
        client_id="<client id issued on QDC.>",
        client_secret="<client secret>",
        base_url="<base endpoint>",
    )
    query_view_connection = SnowflakeConnectionConfig(
        account_id="<snowflake account id. Please use the same account id of metadata agent.>",
        account_role="<account role for query view>",
        account_user="<username>",
        account_password="<password>",
        account_warehouse="<warehouse>",
    )
    is_view_build_only = True  # or False if you want to ingest data.
    execute(
        company_id=company_id,
        sf_build_view_connections=build_view_connection,
        qdc_client=qdc_client,
        is_view_build_only=is_view_build_only,
        sf_query_connections=query_view_connection,
    )
