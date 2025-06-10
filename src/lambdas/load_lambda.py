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

# event = {"files_to_process": [
#     {
#         "table_name": 'dim_counterparty',
#         "key": "2022/11/3/counterparty_2022-11-3_14-20-51_563000.parquet"
#     }
# ]}


def lambda_handler(event: dict, context: EmptyDict):
    # s3_client = boto3.client("s3")
    # PROCESS_ZONE_BUCKET_NAME = os.environ.get("PROCESS_ZONE_BUCKET_NAME")
    # LAMBDA_STATE_BUCKET_NAME = os.environ.get("LAMBDA_STATE_BUCKET_NAME")
    # conn = connect_db("DATAWAREHOUSE")

    files_to_process = orjson.loads(json.dumps(event)).get("files_to_process")
    pprint(files_to_process)
    logger.info("Starting transform process for files")

    """

    dim_to_process = []
    facts_to_process = []

    for file_data in file_to_process:
        # get parquet from s3
        # convert parquet to df
        # check from table name if it's dim or fact and add it to either dim_to_process and facts_to_process
        # make sure that dim_to_process and facts_to_process are a list of dictionaries that contain both the df and the table name

        

    for file_data in dim_to_process:
        create_db_entries_from_df(conn, file_data['table_name'], file_data['df'])
        
    for file_data in facts_to_process:
        create_db_entries_from_df(conn, file_data['table_name'], file_data['df'])


        don't forget to log stuff, like when you start, when you finish, which files were processed into which tables, etc.  
    """

    try:
        pass

    except Exception as err:
        logger.critical(err)
        raise err

    logger.info("Result of loading process:\n%s", pformat(result))
    return orjson.dumps(result)


if __name__ == "__main__":
    result = lambda_handler({}, {})
