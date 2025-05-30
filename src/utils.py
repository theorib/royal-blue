from io import BytesIO

import pandas as pd
from botocore.exceptions import ClientError


def create_parquets_from_data_frames(data: list):
    """
    Converts a list of data frames into in-memory Parquet files.

    This function receives a list of dictionaries where each dictionary represents a table
    with metadata and a pandas DataFrame. It serializes each DataFrame into a compressed
    Parquet file stored in a BytesIO buffer.

    If any conversion fails, the function returns an error dictionary containing the table name
    and the error message. Otherwise, it returns a success dictionary with all converted files.

    Args:
        data (list): A list of dictionaries, each containing:
            - table_name (str): Name of the table.
            - last_updated (str): ISO timestamp of the latest data update.
            - data_frame (pandas.DataFrame): DataFrame to be serialized.

    Returns:
        dict: A JSON-style response indicating success or failure.

        Success:
            {
                "success": {
                    "message": "Parquet file conversion successful.",
                    "data": [
                        {
                            "table_name": "example_table",
                            "last_updated": "2025-05-27 12:00:00",
                            "parquet_file": <_io.BytesIO>
                        },
                        ...
                    ]
                }
            }

        Error:
            {
                "error": {
                    "message": "table_name: error_description"
                }
            }
    """
    parquet_files = []

    for table in data:
        table_name = table.get("table_name")
        last_updated = table.get("last_updated")
        data_frame = table.get("data_frame")

        if not isinstance(data_frame, pd.DataFrame):
            return {"error": {"message": f"{table_name}: invalid data type."}}

        buffer = BytesIO()

        try:
            data_frame.to_parquet(buffer, engine="pyarrow", compression="gzip")
            buffer.seek(0)

            parquet_files.append(
                {
                    "table_name": table_name,
                    "last_updated": last_updated,
                    "parquet_file": buffer,
                }
            )
        except Exception as err:
            return {"error": {"message": f"{table_name}: {err}"}}

    return {
        "success": {
            "message": "Parquet file conversion successful.",
            "data": parquet_files,
        }
    }


def get_file_from_s3_bucket(client, bucket_name, key):
    """Retrieves a file object from the specified S3 bucket.

    This function attempts to download the content of an object stored in an Amazon S3 bucket
    using the given bucket name and object key.

    Args:
        s3_client: A Boto3 S3 client instance used to interact with AWS S3.
        bucket_name: The name of the S3 bucket (string).
        key: The key (i.e., path/filename) of the object to retrieve (string).

    Returns:
        dict: A JSON-style dictionary with the result of the operation, e.g.:
        {
            "success": {
                "message": "File retrieved from s3://bucket_name/key",
                "data": <bytes or string content of the file>
            }
        }
        or
        {
            "error": {
                "message": "Description of the error that occurred"
            }
        }
    """

    try:
        response = client.get_object(Bucket=bucket_name, Key=key)
        status_code = response["ResponseMetadata"]["HTTPStatusCode"]

        if status_code != 200:
            return {"error": {"message": f"{status_code}: Unexpected error"}}

        content = response["Body"].read().decode("utf-8")
        return {
            "success": {
                "message": f"File retrieved from s3://{bucket_name}/{key}",
                "data": content,
            }
        }

    except ClientError as err:
        code = err.response["Error"]["Code"]
        error_map = {
            "NoSuchBucket": "The specified bucket does not exist.",
            "NoSuchKey": "The specified key does not exist.",
            "InvalidBucketName": "The S3 bucket name provided is invalid.",
            "AccessDenied": "Access denied when reading from the S3 bucket.",
            "InternalError": "An internal AWS error occurred. Try again.",
            "SlowDown": "Too many requests sent to S3. Try again later.",
        }
        message = error_map.get(code)
        return {"error": {"message": f"{code}: {message}"}}

    except Exception as ex:
        return {"error": {"message": f"{str(ex)}"}}


def add_to_s3_bucket(client, bucket_name, key, body):
    """Uploads a file or data object to the specified S3 bucket.

    In the first instance, the function is designed to upload content to an Amazon S3 bucket
    using a specified bucket name and object key.

    Args:
        client: a Boto3 S3 client instance used to interact with AWS S3.
        bucket_name: the name of the S3 bucket (string).
        key: the key (i.e., path/filename) to assign to the uploaded object (string).
        body: the content to upload (e.g., string

    Returns:
        dict: A JSON-style dictionary with the result of the operation, e.g.:
        {
            "success": {
                "message": "File uploaded to bucket_name"
                "data": {
                    "Bucket" : bucket_name,
                    "Key" : key
                }
            }
        }
        or
        {
            "error": {
                "message": "Description of the error that occurred"
            }
        }
    """

    try:
        response = client.put_object(Bucket=bucket_name, Key=key, Body=body)
        status_code = response["ResponseMetadata"]["HTTPStatusCode"]

        if status_code != 200:
            return {"error": {"message": f"{status_code}: Unexpected error"}}

        return {
            "success": {
                "message": f"File uploaded to s3://{bucket_name}/{key}",
                "data": {"bucket": bucket_name, "key": key},
            }
        }

    except ClientError as err:
        code = err.response["Error"]["Code"]
        error_map = {
            "NoSuchBucket": "The specified bucket does not exist.",
            "AccessDenied": "Access denied when writing to the S3 bucket.",
            "InvalidBucketName": "The specified bucket is not valid.",
            "BucketAlreadyExists": "The bucket name is already in use.",
            "BucketAlreadyOwnedByYou": "You already own this bucket.",
        }
        message = error_map.get(code)
        return {"error": {"message": f"{code}: {message}"}}

    except Exception as ex:
        return {"error": {"message": f"{str(ex)}"}}
