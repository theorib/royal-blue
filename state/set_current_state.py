import json

from botocore.exceptions import ClientError

from src.utils import add_to_s3_bucket


def set_current_state(current_state: dict, bucket_name, s3_client):
    """
    Converts the given dictionary to JSON and uploads it to S3 as lambda_state.json.
    """
    file_name = 'lambda_state.json'
    json_data = json.dumps(current_state)
    return add_to_s3_bucket(s3_client, bucket_name, file_name, json_data)

          
          

          
