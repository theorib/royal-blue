import json
import logging

from src.utils import add_to_s3_bucket

logger = logging.getLogger(__name__)


def set_current_state(
    current_state: dict, bucket_name, s3_client, file_name="lambda_state.json"
):
    """
    Converts the given dictionary to JSON and uploads it to S3 as lambda_state.json.
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
