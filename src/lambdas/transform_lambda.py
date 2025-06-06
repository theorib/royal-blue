import json
import logging

# import os
# from copy import deepcopy
# from datetime import datetime
from pprint import pformat, pprint

# import boto3
import orjson

from src.utilities.typing_utils import EmptyDict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event: dict, context: EmptyDict):
    # s3_client = boto3.client("s3")
    # INGEST_ZONE_BUCKET_NAME = os.environ.get("INGEST_ZONE_BUCKET_NAME")
    # LAMBDA_STATE_BUCKET_NAME = os.environ.get("LAMBDA_STATE_BUCKET_NAME")
    # result = {"files_to_process": []}
    files_to_process = orjson.loads(json.dumps(event))
    pprint(files_to_process)
    logger.info("Starting transform process for files")

    result = {
        "files_to_process": [],
        "something_else": "I'm some random value from transform_lambda",
    }

    try:
        pass

    except Exception as err:
        logger.critical(err)
        raise err

    logger.info("Result of transform process:\n%s", pformat(result))
    return orjson.dumps(result)


if __name__ == "__main__":
    result = lambda_handler({}, {})
