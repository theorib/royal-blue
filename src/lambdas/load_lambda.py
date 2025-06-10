import json
import logging
import os

# import os
# from copy import deepcopy
# from datetime import datetime
from pprint import pformat

import boto3

# import boto3
import orjson

from src.db.connection import connect_db
from src.utilities.load_lambda_utils import create_db_entries_from_df
from src.utilities.parquets.create_data_frame_from_parquet import (
    create_data_frame_from_parquet,
)
from src.utilities.s3.get_file_from_s3_bucket import get_file_from_s3_bucket
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
    s3_client = boto3.client("s3")
    PROCESS_ZONE_BUCKET_NAME = os.environ.get("PROCESS_ZONE_BUCKET_NAME")
    # LAMBDA_STATE_BUCKET_NAME = os.environ.get("LAMBDA_STATE_BUCKET_NAME")
    conn = connect_db("DATAWAREHOUSE")

    files_to_process = orjson.loads(json.dumps(event)).get("files_to_process")
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

    if not files_to_process:
        logger.info("No files to process")
        return

    try:
        dims_to_process = []
        facts_to_process = []

        for file_data in files_to_process:
            response = get_file_from_s3_bucket(
                s3_client, bucket_name=PROCESS_ZONE_BUCKET_NAME, key=file_data["key"]
            )
            df = create_data_frame_from_parquet(response["success"]["data"])

            if file_data["table_name"].startswith("dim"):
                dims_to_process.append({
                    "table_name": file_data["table_name"],
                    "dataframe": df,
                })
            else:
                facts_to_process.append({
                    "table_name": file_data["table_name"],
                    "dataframe": df,
                })
        with conn:
            for file_data in dims_to_process:
                create_db_entries_from_df(
                    conn, file_data["table_name"], file_data["df"]
                )

            for file_data in facts_to_process:
                create_db_entries_from_df(
                    conn, file_data["table_name"], file_data["df"]
                )

    except Exception as err:
        logger.critical(err)
        raise err

    logger.info("Result of loading process:\n%s", pformat(result))
    return orjson.dumps(result)


if __name__ == "__main__":
    result = lambda_handler({}, {})
