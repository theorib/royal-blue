import logging
from io import BytesIO

import boto3
import pandas as pd
from utilities.extract_dataframe_from_event import extract_dataframes_from_event

from lambdas.dimensions.dim_counterparty_transform import dim_counterparty_dataframe
from lambdas.dimensions.dim_currency_transform import dim_currency_dataframe
from lambdas.dimensions.dim_date_transform import dim_date_dataframe
from lambdas.dimensions.dim_design_transform import dim_design_dataframe
from lambdas.dimensions.dim_location_transform import dim_location_dataframe
from lambdas.dimensions.dim_staff_transform import dim_staff_dataframe
from src.utilities.parquets.create_parquet_from_data_frame import (
    create_parquet_from_data_frame,
)
from utilities.extract_lambda_utils import create_parquet_metadata
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info("Starting Transformation Lambda")
    
    try:
        s3_client = boto3.client('s3')
        cached_dataframes: dict[str, pd.DataFrame] = {} 

        event_dataframes = extract_dataframes_from_event(s3_client, event)
        missing_tables = REQUIRED_TABLES - set(event_dataframes.keys())
        
        if missing_tables:
            logger.info(f"Cache missing tables from s3: {', '.join(missing_tables)}")
        
        for table in missing_tables:
            missing_table_dataframe = cache_missing_table(s3_client, event_dataframes, table)
            cached_dataframes[table] = missing_table_dataframe
            
        # Combine cached_dataframes with event_dataframes
        all_dataframes: dict[str, pd.DataFrame] = {**event_dataframes, **cached_dataframes}
        
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

        dim_counterparty = dim_counterparty_dataframe(all_dataframes)
        dim_location = dim_location_dataframe(all_dataframes)
        dim_currency = dim_currency_dataframe(all_dataframes)
        dim_staff = dim_staff_dataframe(all_dataframes)
        dim_design = dim_design_dataframe(all_dataframes)
        dim_date = dim_date_dataframe(start_date="20100101", end_date="20351231")
        
        # fact_df = fact_df()
        
        # convert to parquets
        files = [dim_counterparty, dim_location, dim_currency, dim_staff, dim_design, dim_date]
        parquets: dict[str, BytesIO] = {}
        for file in files:
            result = create_parquet_from_data_frame(file)
            parquets[file] = result
        
            # filename, key = create_parquet_metadata(
            #     new_table_data_last_updated, file.keys()
            # )
            # Save to s3
        
        # Log results
    except Exception as err:
        logger.critical(f"ERROR: {err}")
        raise err