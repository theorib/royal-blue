import os

from get_file_from_s3_bucket import get_file_from_s3_bucket

from src.utilities.parquets.create_data_frame_from_parquet import (
    create_data_frame_from_parquet,
)
from utilities.state.get_current_state import get_current_state

INGEST_ZONE_BUCKET_NAME = os.environ.get("INGEST_ZONE_BUCKET_NAME")


# ! This function should receive the INGEST_ZONE_BUCKET_NAME bucket as an argument to re-pass it into get_file_from_s3_bucket
# ? Also this function only gets table data it doesn't handle caching which is handled in the next line. It should be called something like get_table_data_from_s3 maybe?
# ! The docstring is not really telling me what this function does, it says it's caching things on s3 but it's just retrieving things.
# ! we need better variable names that makes it easier to understand what this function is doing
def cache_missing_table(client, extracted_dataframes, table_name):
    """
    Handles failed data extraction by caching the problematic table to S3 for debugging or recovery.

    Args:
        client (boto3.client): An instantiated boto3 S3 client used for uploading the file.
        extracted_dataframes (dict): A dictionary of extracted dataframes, typically from a prior step.
        table_name (str): The name of the table whose extraction or processing failed.

    Returns:
        _type_: _description_
    """
    try:
        # Cache
        state = get_current_state(client, INGEST_ZONE_BUCKET_NAME)

        key = state["ingest_state"][f"{table_name}"]["ingest_log"][-1]["key"]

        return create_data_frame_from_parquet(
            get_file_from_s3_bucket(client, INGEST_ZONE_BUCKET_NAME, key)
        )
    except Exception as e:
        raise e
