import logging
import os
from copy import deepcopy
from datetime import datetime
from pprint import pprint,pformat

import boto3

from src.db.connection import connect_db
from src.db.db_helpers import get_table_data, get_totesys_table_names
from src.lambdas.extract_lambda.extract_lambda_utils import (
    create_data_frame_from_list,
    get_last_updated_from_raw_table_data,
    initialize_table_state,
    create_parquet_metadata
)
from src.utilities.parquets.create_parquet_from_data_frame import (
    create_parquet_from_data_frame,
)

from src.utilities.s3.add_file_to_s3_bucket import add_file_to_s3_bucket
from src.utilities.state.get_current_state import get_current_state
from src.utilities.state.set_current_state import set_current_state
from src.utilities.typing_utils import EmptyDict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s: %(message)s",
)
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
    LAMBDA_STATE_BUCKET_NAME = os.environ.get("LAMBDA_STATE_BUCKET_NAME")
    result = {"files_to_process": []}

    try:
        with conn:
            logger.info("Starting extraction process for all tables")

            totesys_tables = get_totesys_table_names(conn)

            for table_name in totesys_tables:
                logger.info(f"Starting extraction of {table_name}")

                current_state = get_current_state(s3_client, LAMBDA_STATE_BUCKET_NAME)
                current_state = initialize_table_state(current_state, table_name)

                current_state_last_updated: datetime | None = current_state[
                    "ingest_state"
                ][table_name]["last_updated"]

                db_response = get_table_data(
                    conn,
                    table_name,  # type: ignore
                    current_state_last_updated,
                )
                extraction_timestamp = datetime.now()

                if db_response.get("error"):
                    raise Exception(db_response["error"]["message"])

                if not len(db_response["success"]["data"]):
                    logger.info(f"No new data to extract from table: {table_name}")
                    continue

                table_data = db_response["success"]["data"]
                new_table_data_last_updated: datetime = (
                    get_last_updated_from_raw_table_data(table_data)
                )

                table_df = create_data_frame_from_list(table_data)
                parquet_file = create_parquet_from_data_frame(table_df)

                response = add_file_to_s3_bucket(
                    s3_client, INGEST_ZONE_BUCKET_NAME, key, parquet_file
                )

                # ! REVIEW should I get raw error obhect from add_file_to_s3_bucket function??
                if response.get("error"):
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

                updated_state_all = deepcopy(current_state)
                updated_state_all["ingest_state"][table_name]["last_updated"] = (
                    #  TODO THEO check unbound error when not ignoring types
                    new_table_data_last_updated  # type: ignore
                )
                updated_state_all["ingest_state"][table_name]["ingest_log"].append(
                    new_state_log_entry  # type: ignore
                )

                set_current_state(
                    updated_state_all, LAMBDA_STATE_BUCKET_NAME, s3_client
                )

                logger.info(f"Finish extracting table:{table_name} data")

        logger.info("Result of extraction process:", pformat({result}))
        logger.info("End of extraction process for all tables")
        return result

    except Exception as err:
        logger.critical(err)
        raise err


if __name__ == "__main__":
    result = lambda_handler({}, {})
    pprint(result)
