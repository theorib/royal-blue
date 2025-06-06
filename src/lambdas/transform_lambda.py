import boto3
import pandas as pd
from utilities.extract_dataframe_from_event import extract_dataframes_from_event

from utilities.s3.get_cache_missing_table import cache_missing_table

REQUIRED_TABLES = {
    'design',
    'counterparty',
    'address',
    'currency',
    'staff',
    'department',
    'transaction',
    'sales_order'
}

def lambda_handler(event, context):
    # TODO:
    # Add logging to handler
    
    s3_client = boto3.client('s3')
    cached_dataframes = {}

    event_dataframes = extract_dataframes_from_event(s3_client, event)
    missing_tables = REQUIRED_TABLES - set(event_dataframes.keys())
    
    for table in missing_tables:
        missing_table_dataframe = cache_missing_table(s3_client, event_dataframes, table)
        cached_dataframes[table] = missing_table_dataframe
        
    # Combine cached_dataframes with event_dataframes
    
    # Transform dataframes into dimensions -> 
    
# import json
# import logging

# # import os
# # from copy import deepcopy
# # from datetime import datetime
# from pprint import pformat, pprint

# # import boto3
# import orjson

# from src.utilities.typing_utils import EmptyDict

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)s: %(message)s",
# )
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)


# def lambda_handler(event: dict, context: EmptyDict):
#     # s3_client = boto3.client("s3")
#     # INGEST_ZONE_BUCKET_NAME = os.environ.get("INGEST_ZONE_BUCKET_NAME")
#     # LAMBDA_STATE_BUCKET_NAME = os.environ.get("LAMBDA_STATE_BUCKET_NAME")
#     # result = {"files_to_process": []}
#     files_to_process = orjson.loads(json.dumps(event))
#     pprint(files_to_process)
#     logger.info("Starting transform process for files")

#     result = {
#         "files_to_process": [],
#         "something_else": "I'm some random value from transform_lambda",
#     }

#     try:
#         pass

#     except Exception as err:
#         logger.critical(err)
#         raise err

#     logger.info("Result of transform process:\n%s", pformat(result))
#     return orjson.dumps(result)


# if __name__ == "__main__":
#     result = lambda_handler({}, {})
