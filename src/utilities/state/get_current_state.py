import logging

import orjson

from src.utilities.s3.add_file_to_s3_bucket import add_file_to_s3_bucket
from src.utilities.s3.get_file_from_s3_bucket import get_file_from_s3_bucket
from src.utilities.s3.s3_error_map import s3_error_map

logger = logging.getLogger(__name__)


def get_current_state(
    s3_client, bucket_name, key="lambda_state.json", s3_error_map=s3_error_map
):
    """
    Connects to S3 and retrieves the current state from lambda_state.json.
    Returns the contents as a Python dictionary.
    """
    try:
        s3_response = get_file_from_s3_bucket(s3_client, bucket_name, key)

        # check if we get an error that says file does not exist
        # if we do, we create a branch new file and return it

        if s3_response.get("error"):
            if (
                s3_response["error"]["message"]
                == f"NoSuchKey: {s3_error_map['NoSuchKey']}"
            ):
                empty_state = {"ingest_state": {}}
                body = orjson.dumps(empty_state)

                add_file_to_s3_bucket(s3_client, bucket_name, key, body)

                return {"ingest_state": {}}

            else:
                raise Exception(s3_response["error"]["message"])

        data = orjson.loads(s3_response["success"]["data"])
        return data

    except orjson.JSONDecodeError as e:
        logger.error(f"ERROR: Invalid JSON in {key}: {e}")
        return {"ingest_state": {}}
