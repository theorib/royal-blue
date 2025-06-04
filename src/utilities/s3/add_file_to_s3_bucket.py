from botocore.exceptions import ClientError


def add_file_to_s3_bucket(client, bucket_name, key, body):
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
        return {"error": {"message": f"{code}: {message}", "raw_response": err}}

    except Exception as err:
        return {"error": {"message": f"{str(err)}", "raw_response": err}}
