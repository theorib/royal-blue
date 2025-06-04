from botocore.exceptions import ClientError

from src.utilities.s3.s3_error_map import s3_error_map


def get_file_from_s3_bucket(client, bucket_name, key, error_map=s3_error_map):
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
        message = error_map.get(code)
        return {"error": {"message": f"{code}: {message}"}}

    except Exception as ex:
        return {"error": {"message": f"{str(ex)}"}}
