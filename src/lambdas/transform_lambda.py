import json
import logging
import os
from copy import deepcopy
from datetime import datetime
from pprint import pprint
from typing import List

import boto3
import orjson
import pandas as pd

from src.utilities.dimensions.dim_counterparty_transform import (
    dim_counterparty_dataframe,
)
from src.utilities.dimensions.dim_currency_transform import dim_currency_dataframe
from src.utilities.dimensions.dim_date_transform import dim_date_dataframe
from src.utilities.dimensions.dim_design_transform import dim_design_dataframe
from src.utilities.dimensions.dim_location_transform import dim_location_dataframe
from src.utilities.dimensions.dim_staff_transform import dim_staff_dataframe
from src.utilities.extract_lambda_utils import create_parquet_metadata
from src.utilities.facts.fact_sales_order_transform import get_fact_sales_order_df
from src.utilities.parquets.create_data_frame_from_parquet import (
    create_data_frame_from_parquet,
)
from src.utilities.parquets.create_parquet_from_data_frame import (
    create_parquet_from_data_frame,
)
from src.utilities.s3.add_file_to_s3_bucket import add_file_to_s3_bucket
from src.utilities.s3.get_file_from_s3_bucket import get_file_from_s3_bucket

# from src.utilities.extract_lambda_utils import create_parquet_metadata
# from src.utilities.parquets.create_parquet_from_data_frame import (
#     create_parquet_from_data_frame,
# )
# from src.utilities.s3.add_file_to_s3_bucket import add_file_to_s3_bucket
# from src.utilities.s3.get_cache_missing_table import cache_missing_table
from src.utilities.state.get_current_state import get_current_state
from src.utilities.state.set_current_state import set_current_state

# from src.utilities.transform_lambda_utils.extract_dataframe_from_event import (
#     extract_dataframes_from_event,
# )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# event = {
#     "files_to_process": [
#         {
#             "table_name": "counterparty",
#             "extraction_timestamp": "2025-06-09T09:51:12.338449",
#             "last_updated": "2022-11-03T14:20:51.563000",
#             "file_name": "counterparty_2022-11-3_14-20-51_563000.parquet",
#             "key": "2022/11/3/counterparty_2022-11-3_14-20-51_563000.parquet",
#         }
#     ]
# }


def lambda_handler(event, context):
    """Pseudo Code suggestion:

        #? what date do we want to use to update state is transform time or is
        new_last_updated = somefunction()

        #? We probably need a function to add a record to dim_date from a datetime object

        all_dims: dict = get_dimensions_from_table_dfs(all_tables_dfs)
        all_facts: dict = get_facts_from_table_dfs(all_tables_dfs)

        facts_and_dims = {[**all_dims,**all_facts}

        #? Maybe we need to process dimensions first here?
        for fact_dim_name, df in facts_and_dims.items():
            logger.info(f"Creating parquet file for {fact_dim_name}")
            parquet = create_parquet_from_data_frame(df)

            # ! is now the right timestamp to pass to the file? genuine question... I don't know
            filename, key = create_parquet_metadata(datetime.now(), df.keys())
            add_file_to_s3_bucket(s3_client, PROCESS_ZONE_BUCKET_NAME, key, parquet)

            state_entry = create_transform_state_log_entry(filename, key, fact_dim_name, some_timestamp?)

            result.append(state_entry)
            final_state[fact_dim_name]['transform_log'].append(state_entry)

        final_state['transform_state']['last_updated'] = new_last_updated


    # if it's not the first time the function runs, process data incrementally
    else:
        # !!!! parquets/df that only have new records are sent to warehouse
        # kind of rinse and repeat some of the previous if statement

    set_current_state(s3_client,LAMBDA_STATE_BUCKET_NAME,final_state)

    return result
    """
    PROCESS_ZONE_BUCKET_NAME = os.environ.get("PROCESS_ZONE_BUCKET_NAME")
    INGEST_ZONE_BUCKET_NAME = os.environ.get("INGEST_ZONE_BUCKET_NAME")
    LAMBDA_STATE_BUCKET_NAME = os.environ.get("LAMBDA_STATE_BUCKET_NAME")

    s3_client = boto3.client("s3")
    logger.info("Starting Transformation Lambda")
    # ! fix event nested lists
    files_to_process: List[dict] = orjson.loads(json.dumps(event)).get(
        "files_to_process"
    )
    print("files to process----")
    pprint(files_to_process)

    current_state: dict = get_current_state(s3_client, LAMBDA_STATE_BUCKET_NAME)
    final_state = deepcopy(current_state)

    result = {"files_to_process": []}

    # result = {"files_to_process": [
    #     {
    #         "table_name": 'dim_counterparty',
    #         "key": "2022/11/3/counterparty_2022-11-3_14-20-51_563000.parquet"
    #     }
    # ]}

    if not files_to_process:
        logger.info("Finish Transformation Lambda with no files to process.")
        return result

    if not current_state.get("transform_state", {}).get("last_updated", None):
        # now we need to process everything
        logger.info(
            "Running transform process for the first time. Gathering data from all tables"
        )

        def get_dataframes_from_files_to_process(client, bucket, files_to_process):
            all_df_to_process = {}
            for file_data in files_to_process:
                table_name = file_data["table_name"]
                key = file_data["key"]

                parquet = get_file_from_s3_bucket(client, bucket, key)
                df = create_data_frame_from_parquet(parquet)

                all_df_to_process[table_name] = df

            return all_df_to_process

        all_tables_dfs: dict = get_dataframes_from_files_to_process(
            s3_client, INGEST_ZONE_BUCKET_NAME, files_to_process
        )

        # TODO create the dim_dates before anything else

        for file_to_process in files_to_process:
            table_name = file_to_process["table_name"]
            last_updated = file_to_process["last_updated"]
            table_name_func_lookup = {
                "design": dim_design_dataframe,
                "counterparty": dim_counterparty_dataframe,
                "currency": dim_currency_dataframe,
                "staff": dim_staff_dataframe,
                "sales_order": get_fact_sales_order_df,
                #! "dim_date":???????
            }
            prefix = "fact_" if "fact" in table_name_func_lookup[table_name] else "dim_"
            new_table_name = prefix + table_name

            df = table_name_func_lookup[table_name](all_tables_dfs)
            parquet = create_parquet_from_data_frame(df)

            # ! if we are processing a fact, we need to send to bucket both dim_date and fact_sales_order
            filename, key = create_parquet_metadata(last_updated, new_table_name)

            response = add_file_to_s3_bucket(
                s3_client, PROCESS_ZONE_BUCKET_NAME, key, parquet
            )

            if response.get("error"):
                raise Exception("something went wring with uploading")

            log_item = {
                "table_name": new_table_name,
                "key": key,
                "filename": filename,
                "last_updated": last_updated,
                "transformation_timestamp": datetime.now(),
                "transformation_input": file_to_process,
            }

            result["files_to_process"].append(log_item)

            # TODO update_state

        return result

    else:
        # now we only process incrementally
        pass

    set_current_state(final_state, s3_client, LAMBDA_STATE_BUCKET_NAME)
    return orjson.dumps(result)


"""
    required_tables = {
        "design",
        "counterparty",
        "address",
        "currency",
        "staff",
        "department",
        "transaction",
        "sales_order",
    }

    logger.info("Starting Transformation Lambda")
    PROCESS_ZONE_BUCKET_NAME = os.environ.get("PROCESS_ZONE_BUCKET_NAME")
    # INGEST_ZONE_BUCKET_NAME = os.environ.get("INGEST_ZONE_BUCKET_NAME")
    # LAMBDA_STATE_BUCKET_NAME = os.environ.get("LAMBDA_STATE_BUCKET_NAME")

    try:
        s3_client = boto3.client("s3")
        # ? Maybe the Cache could be the ingest_state that we always have available
        cached_dataframes: dict[str, pd.DataFrame] = {}

        # ? Maybe Event tables could have a better name like incoming or to_transform or transform_queue table??
        # ? Maybe we could change the word extract to get to keep consistency with the rest of the project
        # ! This function should receive the INGEST_ZONE_BUCKET_NAME bucket as an argument to re-pass it into get_file_from_s3_bucket
        event_dataframes = extract_dataframes_from_event(s3_client, event)
        missing_tables = required_tables - set(event_dataframes.keys())

        if missing_tables:
            logger.info(f"Cache missing tables from s3: {', '.join(missing_tables)}")

        for table in missing_tables:
            # ! This function should receive the INGEST_ZONE_BUCKET_NAME bucket as an argument to re-pass it into get_file_from_s3_bucket
            # ? Also this function only gets table data it doesn't handle caching which is handled in the next line. It should be called something like get_table_data_from_s3 maybe?
            missing_table_dataframe = cache_missing_table(
                s3_client, event_dataframes, table
            )
            cached_dataframes[table] = missing_table_dataframe

        # Combine cached_dataframes with event_dataframes
        all_dataframes: dict[str, pd.DataFrame] = {
            **event_dataframes,
            **cached_dataframes,
        }

        # Transform dataframes into dimensions -> facts
        # ? since we are passing all_dataframes to all the helper functions, does this mean that we are allways processing all data all over again?
        # ? Maybe we are missing a logic to handle incremental steps
        dim_counterparty = dim_counterparty_dataframe(all_dataframes)
        dim_location = dim_location_dataframe(all_dataframes)
        dim_currency = dim_currency_dataframe(all_dataframes)
        dim_staff = dim_staff_dataframe(all_dataframes)
        dim_design = dim_design_dataframe(all_dataframes)
        # ? Do we need to create an enourmous amout of date entries? all we need is entries for the date in our database...
        dim_date = dim_date_dataframe(start_date="20100101", end_date="20351231")
        # fact_df = fact_df()

        dim_dfs = [
            dim_counterparty,
            dim_location,
            dim_currency,
            dim_staff,
            dim_design,
            dim_date,
        ]
        files = []

        # convert to parquets and save into s3
        for df in dim_dfs:
            parquet = create_parquet_from_data_frame(df)

            # ! is now the right timestamp to pass to the file? genuine question... I don't know
            _, key = create_parquet_metadata(datetime.now(), df.keys())
            add_file_to_s3_bucket(s3_client, PROCESS_ZONE_BUCKET_NAME, key, parquet)
            files.append({df: parquet, "key": key})

        logger.info("Transform Complete")
        return files

    except Exception as err:
        logger.critical(f"ERROR: {err}")
        raise err
    this may  be what logs look like for transform?
    {
       "table_name": "fact_sales_order",
       "file_name": fact_sales_order_2025-6-6_17-14-9_783000.parquet
       "last_updated": "2025-05-27 15:24:07.582020"
       "key": "2025/6/6/fact_sales_order_2025-6-6_17-14-9_783000.parquet"
       "source_files": [
          {
                   "table_name": "sales_order",
                   "extraction_timestamp": "2025-06-06T17:24:03.378788",
                   "last_updated": "2025-06-06T17:14:09.783000",
                   "file_name": "sales_order_2025-6-6_17-14-9_783000.parquet",
                   "key": "2025/6/6/sales_order_2025-6-6_17-14-9_783000.parquet"
           },
           {
                   "table_name": "address",
                   "extraction_timestamp": "2025-06-06T10:17:48.861703",
                   "last_updated": "2022-11-03T14:20:49.962000",
                   "file_name": "address_2022-11-3_14-20-49_962000.parquet",
                   "key": "2022/11/3/address_2022-11-3_14-20-49_962000.parquet"
           }
       ]
    
    }
"""


if __name__ == "__main__":
    print("python Dictionary ---------")
    original_return_value = [
        {
            "table_name": "currency",
            "extraction_timestamp": datetime.now(),
            "last_updated": datetime.now(),
            "file_name": "filename",
            "key": "key",
        }
    ]
    pprint(original_return_value)
    print("event ---------")
    event = orjson.dumps(original_return_value)

    result = lambda_handler(json.loads(event), {})
