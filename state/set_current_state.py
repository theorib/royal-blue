import json
import logging

from src.utils import add_to_s3_bucket

logger = logging.getLogger(__name__)


def set_current_state(
    current_state: dict, bucket_name, s3_client, file_name="lambda_state.json"
):
    """
    Converts the current state dictionary to JSON and uploads it to an S3 bucket as a state file.

    Args:
    current_state (dict): The current state to upload, as a dictionary.
    bucket_name (str): The name of the S3 bucket where the state will be stored.
    s3_client (Any): boto3 S3 client used for the upload.
    file_name (str, optional): The name of the file to upload. Defaults to 'lambda_state.json'.

    Returns:
    dict: Result of the S3 upload operation. On success, contains info about the upload. On failure, contains error details.

    Raises:
    Exception: If the upload to S3 fails.
    """ 
    try:
        json_data = json.dumps(current_state)
        result = add_to_s3_bucket(s3_client, bucket_name, file_name, json_data)

        if result.get("error"):
            logger.error(f"Failed to update state in S3: {result['error']}")
            raise Exception("Failed to upload current state to S3.")
        return result

    except Exception as e:
        logger.error(f"Failed to set current state: {e}")
        raise
