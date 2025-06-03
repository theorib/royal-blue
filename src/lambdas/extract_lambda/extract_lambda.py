import logging
import os
from copy import deepcopy
from datetime import datetime
from pprint import pprint

import boto3

from src.db.connection import connect_db
from src.db.db_helpers import get_table_data, get_totesys_table_names
from src.lambdas.extract_lambda.create_parquet_from_data_frame import (
    create_parquet_from_data_frame,
)
from src.lambdas.extract_lambda.extract_lambda_utils import (
    create_data_frame_from_list,
    get_last_updated_from_raw_table_data,
)
from src.typing_utils import EmptyDict
from src.utils import add_to_s3_bucket

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event: EmptyDict, context: EmptyDict):
    """
    1. create a db connection
    2. create an s3 client
    3. get LAMBDA_STATE_BUCKET_NAME
    4. get INGEST_ZONE_BUCKET_NAME

    5. get current_state
      if current_state file does not exist INITIALIZE STATE:
      create a new current state file with {"ingest_state": {}}

    6. get all table names from Totesys db
    7. for each table:
    check state for this table

    IF there is no state for this table
    we download all data for this table in following steps
    IF there is state for this table, we get last_updated for this table
    on next steps we get data from db updated after last_updated timestamp

    get table data from db (with or without last_updated filter)

    get last_updated from the data we just got

    create a data frame from table data
    create a parquet from data frame
    upload parquet to s3

    update state entry for that table:
    update last_updated
    append to ingest_log
      filename
      s3_key
      last_updated

    """

    conn = connect_db()
    s3_client = boto3.client("s3")
    INGEST_ZONE_BUCKET_NAME = os.environ.get("INGEST_ZONE_BUCKET_NAME")
    # LAMBDA_STATE_BUCKET_NAME = os.environ.get("LAMBDA_STATE_BUCKET_NAME")

    # get current_state
    # if current_state file does not exist INITIALIZE STATE

    # open db connection context manager
    with conn:
        totesys_tables = get_totesys_table_names(conn)

        result = {"files_to_process": []}

        for table_name in totesys_tables:
            # ! CHANGE THIS TO CALLING GET STATE FUNCTION
            current_state = {"ingest_state": {}}
            # this should be it's own function (get_table_state(state, table_name))

            if not current_state.get("ingest_state", {}).get(table_name):
                current_state["ingest_state"][table_name] = {
                    "last_updated": None,
                    "ingest_log": [],
                }

            current_state_last_updated: datetime | None = current_state["ingest_state"][
                table_name
            ]["last_updated"]

            db_response = get_table_data(conn, table_name, current_state_last_updated)
            extraction_timestamp = datetime.now()

            if db_response.get("success") and len(db_response["success"]["data"]):
                table_data = db_response["success"]["data"]

                new_table_data_last_updated: datetime = (
                    get_last_updated_from_raw_table_data(table_data)
                )

                table_df = create_data_frame_from_list(table_data)
                parquet_file = create_parquet_from_data_frame(table_df)

                year = new_table_data_last_updated.year
                month = new_table_data_last_updated.month
                day = new_table_data_last_updated.day

                # currency_2025-06-13_10-35-20_012023.parquet
                filename = f"{table_name}_{year}-{month}-{day}_{new_table_data_last_updated.hour}-{new_table_data_last_updated.minute}-{new_table_data_last_updated.second}_{new_table_data_last_updated.microsecond}.parquet"

                # 2025/06/13/currency_2025-06-13_10-35-20_012023.parquet
                key = f"{year}/{month}/{day}/{filename}"

                response = add_to_s3_bucket(
                    s3_client, INGEST_ZONE_BUCKET_NAME, key, parquet_file
                )

                if response.get("error"):
                    logger.error(response["error"]["message"])
                    raise response["error"]["raw_response"]  # type: ignore

                new_state_log_entry = (
                    {
                        "table_name": table_name,
                        "extraction_timestamp": extraction_timestamp,
                        "last_updated": new_table_data_last_updated,
                        "file_name": filename,
                        "key": key,
                    },
                )

                result["files_to_process"].append(new_state_log_entry)

                # ! CHANGE THIS TO CALLING THE SET STATE FUNCTION
                # update state
                updated_state_all = deepcopy(current_state)
                updated_state_all["ingest_state"][table_name]["last_updated"] = (
                    new_table_data_last_updated
                )
                updated_state_all["ingest_state"][table_name]["ingest_log"].append(
                    new_state_log_entry
                )

                # !! upload updated_state_all to s3

                # update results

        return result


if __name__ == "__main__":
    result = lambda_handler({}, {})
    pprint(result)
