import json

from botocore.exceptions import ClientError

from src.utils import get_file_from_s3_bucket


def get_current_state(s3_client, bucket_name):
    """
    Connects to S3 and retrieves the current state from lambda_state.json.
    Returns the contents as a Python dictionary.
    """
    key = 'lambda_state.json'

    try:
        content = get_file_from_s3_bucket(s3_client, bucket_name, key)
        if "error" in content:
            return content
        return {
            "success": {
                "message": f"{key} read from S3",
                "data": json.loads(content["success"]["data"])
            }       
        }
    # return empty {"ingest_state": {}}

    except ClientError as e:
          if e.response["Error"]["Code"] == "NoSuchKey":
              return {
                "error": {
                    "message": f"{e}"
                }
            }
    except json.JSONDecodeError as e:
         return {
              "error": {
                   "message": f"Invalid JSON in {key}: {e}"
              }
         }