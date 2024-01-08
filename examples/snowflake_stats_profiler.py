from quollio_core.repository import qdc, snowflake
from quollio_core.snowflake_stats_profiler import execute


def main():
    """
    You can create views for collecting stats in your snowflake account.
    Data ingesting will be skipped, just creating views.
    """
    company_id = "<your company id>"
    sf_build_view_connections = snowflake.SnowflakeConnectionConfig(
        account_id="<snowflake account id. Please use the same account id of metadata agent.>",
        account_role="<account role for build view>",
        account_user="<username>",
        account_password="<password>",
        account_warehouse="<warehouse>",
    )
    qdc_client = qdc.QDCExternalAPIClient(
        client_id="<client id issued on QDC.>",
        client_secret="<client secret>",
        base_url="<base endpoint>",
    )
    query_view_connection = snowflake.SnowflakeConnectionConfig(
        account_id="<snowflake account id. Please use the same account id of metadata agent.>",
        account_role="<account role for query view>",
        account_user="<username>",
        account_password="<password>",
        account_warehouse="<warehouse>",
    )
    is_view_build_only = True  # or False if you want to ingest data.
    stats_databases = ["<the databases you want to get stats from.>"]  # or []

    # The amount of stats calculation is large, then use sampling method when creating view.
    # You can specify sampling method. Default value is SAMPLE(10).
    # Please refer to the docs(https://docs.snowflake.com/en/sql-reference/constructs/sample).
    stats_sample_method = "SAMPLE(10)"
    execute(
        company_id=company_id,
        sf_build_view_connections=sf_build_view_connections,
        qdc_client=qdc_client,
        is_view_build_only=is_view_build_only,
        sf_query_connections=query_view_connection,
        stats_target_databases=stats_databases,
        stats_sample_method=stats_sample_method,
    )
