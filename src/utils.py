import boto3
from botocore.exceptions import ClientError

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
            "Success": {
                "Message": "File uploaded to bucket_name"
                "Data": ""
            }
        }
        or
        {
            "Error": {
                "Message": "Description of the error that occurred"
            }
        }
    """
    try:
        client.put_object(Bucket=bucket_name, Key=key, Body=body)

        return {
            "Success": {
                "Message": f"File uploaded to s3://{bucket_name}/{key}",
                "Data": {
                    "Bucket": bucket_name,
                    "Key": key
                }
            }
        }
    except ClientError as err:
        code = err.response['Error']['Code']

        error_map = {
            "NoSuchBucket": "The specified bucket does not exist.",
            "AccessDenied": "Access denied when writing to the S3 bucket.",
            "InvalidBucketName": "The specified bucket is not valid.",
            "BucketAlreadyExists": "The bucket name is already in use.",
            "BucketAlreadyOwnedByYou": "You already own this bucket."
        }
        message = error_map.get(code)
            
        return {
            "Error": { 
                "Message": f"{code}: {message}"
            }
        }
    
    