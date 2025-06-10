import boto3

from src.utilities.parquets.create_data_frame_from_parquet import (
    create_data_frame_from_parquet,
)
from src.utilities.s3.get_file_from_s3_bucket import get_file_from_s3_bucket

BUCKET_NAME = "ingestion-zone-20250530151335299400000005"
client = boto3.client("s3")


def extract_dataframes_from_event(client, event):
    """
    Extracts data frames from a list of event table metadata by retrieving Parquet files
    from S3 and converting them to Pandas DataFrames.

    Parameters:
    ----------
    client : boto3.client
        The S3 client used to fetch the files.

    event : list of dict
        A list of dictionaries, each representing a table to be extracted.
        Each dictionary must contain the following keys:
            - 'table_name' (str): The name of the table.
            - 'extraction_timestamp' (str): The timestamp of data extraction.
            - 'last_updated' (str): The last updated timestamp for the table data.
            - 'file_name' (str): The file name of the Parquet file in S3.
            - 'key' (str): The S3 key (path) to the Parquet file.

    Returns:
    -------
    dict
        A dictionary where each key is a table name (str) and the value is the
        corresponding Pandas DataFrame extracted from the Parquet file.

    Raises:
    ------
    Exception
        If any error occurs while processing any of the tables in the event list.
    """
    extracted_data_frames = {}

    for table in event:
        key = table["key"]
        table_name = table["table_name"]

        try:
            s3_result = get_file_from_s3_bucket(client, BUCKET_NAME, key)

            if not s3_result.get("success") or "data" not in s3_result["success"]:
                raise Exception(s3_result["error"]["message"])

            parquet_bytes = s3_result["success"]["data"]
            data_frame = create_data_frame_from_parquet(parquet_bytes)
            extracted_data_frames[table_name] = data_frame

        except Exception as e:
            raise Exception(f"Error processing table '{table_name}': {e}")

    return extracted_data_frames
