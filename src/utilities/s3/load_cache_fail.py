import os

from get_file_from_s3_bucket import get_file_from_s3_bucket

from src.utilities.transform_lambda_utils.convert_parquet_to_dataframe import (
    parquet_to_dataframe,
)
from utilities.state.get_current_state import get_current_state

INGEST_ZONE_BUCKET_NAME = os.environ.get("INGEST_ZONE_BUCKET_NAME")
INGEST_PREFIX = os.environ.get("")


def s3_load_cache_fail(client, extracted_dataframes, table_name):
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

        return parquet_to_dataframe(
            get_file_from_s3_bucket(client, INGEST_ZONE_BUCKET_NAME, key)
        )
    except Exception as e:
        raise e
