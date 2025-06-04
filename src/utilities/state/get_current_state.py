import json
import logging

from botocore.exceptions import ClientError

from src.utilities.s3.get_file_from_s3_bucket import get_file_from_s3_bucket

logger = logging.getLogger(__name__)


def get_current_state(s3_client, bucket_name, key="lambda_state.json"):
    """
    Connects to S3 and retrieves the current state from lambda_state.json.
    Returns the contents as a Python dictionary.
    """
    try:
        content = get_file_from_s3_bucket(s3_client, bucket_name, key)

        if content.get("error"):
            error = ClientError(
                {
                    "Error": {
                        "Code": "InvalidContent",
                        "Message": "'error' found in file content",
                    }
                },
                "GetObject",
            )
            logger.error(error)
            return {"ingest_state": {}}

        data = json.loads(content["success"]["data"])
        return data

    except json.JSONDecodeError as e:
        logger.error(f"ERROR: Invalid JSON in {key}: {e}")
        return {"ingest_state": {}}
