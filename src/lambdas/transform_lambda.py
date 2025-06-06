import logging
import os
from datetime import datetime

import boto3
import pandas as pd

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
from utilities.s3.add_file_to_s3_bucket import add_file_to_s3_bucket
from utilities.s3.get_cache_missing_table import cache_missing_table
from utilities.transform_lambda_utils.extract_dataframe_from_event import (
    extract_dataframes_from_event,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
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
    PROCESSED_BUCKET = os.environ.get("INGEST_ZONE_BUCKET_NAME")

    try:
        s3_client = boto3.client("s3")
        cached_dataframes: dict[str, pd.DataFrame] = {}

        event_dataframes = extract_dataframes_from_event(s3_client, event)
        missing_tables = required_tables - set(event_dataframes.keys())

        if missing_tables:
            logger.info(f"Cache missing tables from s3: {', '.join(missing_tables)}")

        for table in missing_tables:
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
        dim_counterparty = dim_counterparty_dataframe(all_dataframes)
        dim_location = dim_location_dataframe(all_dataframes)
        dim_currency = dim_currency_dataframe(all_dataframes)
        dim_staff = dim_staff_dataframe(all_dataframes)
        dim_design = dim_design_dataframe(all_dataframes)
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

            _, key = create_parquet_metadata(datetime.now(), df.keys())
            add_file_to_s3_bucket(s3_client, PROCESSED_BUCKET, key, parquet)
            files.append({df: parquet, "key": key})

        logger.info("Transform Complete")
        return files

    except Exception as err:
        logger.critical(f"ERROR: {err}")
        raise err
