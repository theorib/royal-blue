import json
import logging
import os
from copy import deepcopy
from datetime import datetime
from typing import List

import boto3
import orjson

from src.utilities.dimensions.dim_counterparty_transform import (
    dim_counterparty_dataframe,
)
from src.utilities.dimensions.dim_currency_transform import dim_currency_dataframe
from src.utilities.dimensions.dim_date_transform import dim_date_dataframe
from src.utilities.dimensions.dim_design_transform import dim_design_dataframe
from src.utilities.dimensions.dim_staff_transform import dim_staff_dataframe
from src.utilities.extract_lambda_utils import create_parquet_metadata
from src.utilities.facts.create_fact_sales_order_from_df import (
    create_fact_sales_order_from_df,
)
from src.utilities.parquets.create_data_frame_from_parquet import (
    create_data_frame_from_parquet,
)
from src.utilities.parquets.create_parquet_from_data_frame import (
    create_parquet_from_data_frame,
)
from src.utilities.s3.add_file_to_s3_bucket import add_file_to_s3_bucket
from src.utilities.s3.get_file_from_s3_bucket import get_file_from_s3_bucket
from src.utilities.state.get_current_state import get_current_state
from src.utilities.state.set_current_state import set_current_state

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

# result = {"files_to_process": [
#     {
#         "table_name": 'dim_counterparty',
#         "key": "2022/11/3/dim_counterparty_2022-11-3_14-20-51_563000.parquet"
#     }
# ]}


def lambda_handler(event, context):
    PROCESS_ZONE_BUCKET_NAME = os.environ.get("PROCESS_ZONE_BUCKET_NAME")
    INGEST_ZONE_BUCKET_NAME = os.environ.get("INGEST_ZONE_BUCKET_NAME")
    LAMBDA_STATE_BUCKET_NAME = os.environ.get("LAMBDA_STATE_BUCKET_NAME")

    s3_client = boto3.client("s3")
    logger.info("Starting Transformation Lambda")

    # ! fix datetime not coming in
    files_to_process: List[dict] = orjson.loads(json.dumps(event)).get(
        "files_to_process"
    )

    current_state: dict = get_current_state(s3_client, LAMBDA_STATE_BUCKET_NAME)

    final_state = deepcopy(current_state)

    result = {"files_to_process": []}

    if not files_to_process:
        logger.info("Finish Transformation Lambda with no files to process.")
        return result

    if not current_state.get("transform_state", {}).get("last_updated", None):
        logger.info(
            "Running transform process for the first time. Gathering data from all tables"
        )

        # TODO this function needs to move to it's own file
        def get_dataframes_from_files_to_process(client, bucket, files_to_process):
            all_df_to_process = {}
            for file_data in files_to_process:
                table_name = file_data["table_name"]
                key = file_data["key"]

                response = get_file_from_s3_bucket(client, bucket, key)

                parquet = response["success"]["data"]

                df = create_data_frame_from_parquet(parquet)

                all_df_to_process[table_name] = df

            return all_df_to_process

        all_tables_dfs: dict = get_dataframes_from_files_to_process(
            s3_client, INGEST_ZONE_BUCKET_NAME, files_to_process
        )

        # TODO create the dim_dates before anything else
        # !inject dim_date into files to process
        inject_dim_date = {
            "extraction_timestamp": None,
            "file_name": None,
            "key": None,
            "last_updated": datetime.now().isoformat(),
            "table_name": "dim_date",
        }

        files_to_process.append(inject_dim_date)

        # !initialize state
        final_state["transform_state"] = {"last_updated": None, "tables": {}}
        if final_state:
            for file_to_process in files_to_process:
                table_name: str = file_to_process["table_name"]
                final_state["transform_state"]["tables"][table_name] = {}

        for file_to_process in files_to_process:
            table_name = file_to_process["table_name"]
            last_updated = file_to_process["last_updated"]

            # ! these are the tables we are still missing fro post mvp
            # purchase_order
            # payment_type
            # payment
            # transaction

            match table_name:
                case "counterparty":
                    df = dim_counterparty_dataframe(
                        counterparty=all_tables_dfs["counterparty"],
                        address=all_tables_dfs["address"],
                    )
                    prefix = "dim_"
                case "design":
                    df = dim_design_dataframe(
                        design=all_tables_dfs["design"],
                    )
                    prefix = "dim_"
                case "currency":
                    df = dim_currency_dataframe(currency=all_tables_dfs["currency"])
                    prefix = "dim_"
                case "staff":
                    df = dim_staff_dataframe(
                        department=all_tables_dfs["department"],
                        staff=all_tables_dfs["staff"],
                    )
                    prefix = "dim_"
                case "dim_date":
                    df = dim_date_dataframe("20221102", "20500101")
                    prefix = ""
                case "sales_order":
                    df = create_fact_sales_order_from_df(all_tables_dfs["sales_order"])
                    prefix = "fact_"
                case _:
                    continue

            new_table_name = prefix + table_name

            parquet = create_parquet_from_data_frame(df)

            filename, key = create_parquet_metadata(
                # ! we need to fix the json import from event to give us datetime objects
                datetime.fromisoformat(last_updated),
                new_table_name,
            )

            response = add_file_to_s3_bucket(
                s3_client, PROCESS_ZONE_BUCKET_NAME, key, parquet
            )

            if response.get("error"):
                raise Exception("something went wring with uploading")

            transformation_timestamp = datetime.now()

            log_item = {
                "table_name": new_table_name,
                "key": key,
                "filename": filename,
                # ! we need to fix the json import from event to give us datetime objects
                "last_updated": datetime.fromisoformat(last_updated),
                "transformation_timestamp": transformation_timestamp,
                # TODO take into account that some functions use more than one input
                # "transformation_input": file_to_process,
            }

            result["files_to_process"].append(log_item)

            final_state["transform_state"]["tables"][table_name][
                "transformation_timestamp"
            ] = transformation_timestamp
            final_state["transform_state"]["tables"][table_name]["process_log"] = [
                log_item
            ]

            final_state["transform_state"]["last_updated"] = transformation_timestamp

    else:
        # now we only process incrementally
        pass

    set_current_state(final_state, LAMBDA_STATE_BUCKET_NAME, s3_client)

    return orjson.dumps(result)


if __name__ == "__main__":
    test_event = {
        "files_to_process": [
            {
                "table_name": "counterparty",
                "extraction_timestamp": "2025-06-10T20:51:34.260407",
                "last_updated": "2022-11-03T14:20:51.563000",
                "file_name": "counterparty_2022-11-3_14-20-51_563000.parquet",
                "key": "2022/11/3/counterparty_2022-11-3_14-20-51_563000.parquet",
            },
            {
                "table_name": "address",
                "extraction_timestamp": "2025-06-10T20:51:37.179714",
                "last_updated": "2022-11-03T14:20:49.962000",
                "file_name": "address_2022-11-3_14-20-49_962000.parquet",
                "key": "2022/11/3/address_2022-11-3_14-20-49_962000.parquet",
            },
            {
                "table_name": "department",
                "extraction_timestamp": "2025-06-10T20:51:39.879502",
                "last_updated": "2022-11-03T14:20:49.962000",
                "file_name": "department_2022-11-3_14-20-49_962000.parquet",
                "key": "2022/11/3/department_2022-11-3_14-20-49_962000.parquet",
            },
            {
                "table_name": "purchase_order",
                "extraction_timestamp": "2025-06-10T20:51:42.599581",
                "last_updated": "2025-06-10T16:51:09.789000",
                "file_name": "purchase_order_2025-6-10_16-51-9_789000.parquet",
                "key": "2025/6/10/purchase_order_2025-6-10_16-51-9_789000.parquet",
            },
            {
                "table_name": "staff",
                "extraction_timestamp": "2025-06-10T20:51:46.181541",
                "last_updated": "2022-11-03T14:20:51.563000",
                "file_name": "staff_2022-11-3_14-20-51_563000.parquet",
                "key": "2022/11/3/staff_2022-11-3_14-20-51_563000.parquet",
            },
            {
                "table_name": "payment_type",
                "extraction_timestamp": "2025-06-10T20:51:48.938756",
                "last_updated": "2022-11-03T14:20:49.962000",
                "file_name": "payment_type_2022-11-3_14-20-49_962000.parquet",
                "key": "2022/11/3/payment_type_2022-11-3_14-20-49_962000.parquet",
            },
            {
                "table_name": "payment",
                "extraction_timestamp": "2025-06-10T20:51:52.380596",
                "last_updated": "2025-06-10T18:01:10.155000",
                "file_name": "payment_2025-6-10_18-1-10_155000.parquet",
                "key": "2025/6/10/payment_2025-6-10_18-1-10_155000.parquet",
            },
            {
                "table_name": "transaction",
                "extraction_timestamp": "2025-06-10T20:51:59.679060",
                "last_updated": "2025-06-10T18:01:10.155000",
                "file_name": "transaction_2025-6-10_18-1-10_155000.parquet",
                "key": "2025/6/10/transaction_2025-6-10_18-1-10_155000.parquet",
            },
            {
                "table_name": "design",
                "extraction_timestamp": "2025-06-10T20:52:12.199106",
                "last_updated": "2025-06-10T17:51:09.671000",
                "file_name": "design_2025-6-10_17-51-9_671000.parquet",
                "key": "2025/6/10/design_2025-6-10_17-51-9_671000.parquet",
            },
            {
                "table_name": "sales_order",
                "extraction_timestamp": "2025-06-10T20:52:15.981714",
                "last_updated": "2025-06-10T18:01:10.155000",
                "file_name": "sales_order_2025-6-10_18-1-10_155000.parquet",
                "key": "2025/6/10/sales_order_2025-6-10_18-1-10_155000.parquet",
            },
            {
                "table_name": "currency",
                "extraction_timestamp": "2025-06-10T20:52:22.062585",
                "last_updated": "2022-11-03T14:20:49.962000",
                "file_name": "currency_2022-11-3_14-20-49_962000.parquet",
                "key": "2022/11/3/currency_2022-11-3_14-20-49_962000.parquet",
            },
        ]
    }

    # result = lambda_handler(test_event, {})
    # pprint(result)
