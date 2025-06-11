import json
import logging
import os
from copy import deepcopy
from datetime import datetime

import boto3
import orjson

from src.utilities.dimensions.dim_counterparty_transform import (
    dim_counterparty_dataframe,
)
from src.utilities.dimensions.dim_currency_transform import dim_currency_dataframe
from src.utilities.dimensions.dim_date_transform import dim_date_dataframe
from src.utilities.dimensions.dim_design_transform import dim_design_dataframe
from src.utilities.dimensions.dim_location_transform import dim_location_dataframe
from src.utilities.dimensions.dim_staff_transform import dim_staff_dataframe
from src.utilities.facts.create_fact_sales_order_from_df import (
    create_fact_sales_order_from_df,
)
from src.utilities.parquets.create_parquet_from_data_frame import (
    create_parquet_from_data_frame,
)
from src.utilities.pydantic_models import (
    FilesToProcessList,
    State,
)
from src.utilities.state.get_current_state import get_current_state
from src.utilities.state.set_current_state import set_current_state
from src.utilities.transform_lambda_utils.transform_lambda_utils import (
    add_log_to_result_and_state,
    get_dataframes_from_files_to_process,
    get_log_item_df_s3_upload,
    initialize_dim_date,
    initialize_transform_state,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    PROCESS_ZONE_BUCKET_NAME = os.environ.get("PROCESS_ZONE_BUCKET_NAME")
    INGEST_ZONE_BUCKET_NAME = os.environ.get("INGEST_ZONE_BUCKET_NAME")
    LAMBDA_STATE_BUCKET_NAME = os.environ.get("LAMBDA_STATE_BUCKET_NAME")

    s3_client = boto3.client("s3")
    logger.info("Starting Transformation Lambda")

    # # ! fix datetime not coming in
    files_to_process = FilesToProcessList.validate_python(
        orjson.loads(json.dumps(event)).get("files_to_process")
    )
    current_state = State.model_validate(
        get_current_state(s3_client, LAMBDA_STATE_BUCKET_NAME)
    ).model_dump()

    final_state = deepcopy(current_state)

    result: dict[str, list[dict]] = {"files_to_process": []}

    if not files_to_process:
        logger.info("Finish Transformation Lambda with no files to process.")
        return result

    if not current_state.get("transform_state", {}).get("last_updated", None):
        logger.info(
            "Running transform process for the first time. Initializing dim_date table"
        )

        final_state = initialize_transform_state(
            final_state, [file.table_name for file in files_to_process]
        )

        initialize_dim_date(
            create_dim_date_df_func=dim_date_dataframe,  # type: ignore
            s3_client=s3_client,
            bucket_name=PROCESS_ZONE_BUCKET_NAME,  # type: ignore
            result=result,
            final_state=final_state,
        )

    logger.info(
        f"Running transform process for {[entry.table_name for entry in files_to_process]} table(s)."
    )

    all_tables_dfs: dict = get_dataframes_from_files_to_process(
        s3_client,
        INGEST_ZONE_BUCKET_NAME,  # type: ignore
        files_to_process,
    )

    for file_to_process in files_to_process:
        table_name = file_to_process.table_name
        last_updated = file_to_process.last_updated

        logger.info(f"Running transform on {table_name}.")

        match table_name:
            case "counterparty":
                df = dim_counterparty_dataframe(
                    counterparty=all_tables_dfs["counterparty"],
                    address=all_tables_dfs["address"],
                )
                new_table_name = "dim_counterparty"
            case "design":
                df = dim_design_dataframe(
                    design=all_tables_dfs["design"],
                )
                new_table_name = "dim_design"
            case "currency":
                df = dim_currency_dataframe(currency=all_tables_dfs["currency"])
                new_table_name = "dim_currency"
            case "staff":
                df = dim_staff_dataframe(
                    department=all_tables_dfs["department"],
                    staff=all_tables_dfs["staff"],
                )
                new_table_name = "dim_staff"
            case "dim_date":
                df = dim_date_dataframe("20221102", "20500101")
                new_table_name = "dim_date"
            case "address":
                df = dim_location_dataframe(address=all_tables_dfs["address"])
                new_table_name = "dim_location"
            case "sales_order":
                df = create_fact_sales_order_from_df(all_tables_dfs["sales_order"])
                new_table_name = "fact_sales_order"
            case _:
                continue
        transformation_timestamp = datetime.now()

        log_item = get_log_item_df_s3_upload(
            s3_client=s3_client,
            bucket_name=PROCESS_ZONE_BUCKET_NAME,  # type: ignore
            last_updated=last_updated,
            new_table_name=new_table_name,
            df=df,
            transformation_timestamp=transformation_timestamp,
            create_parquet_from_df_func=create_parquet_from_data_frame,  # type: ignore
        )
        add_log_to_result_and_state(
            log=log_item,  # type: ignore
            result=result,
            state=final_state,
            last_updated=transformation_timestamp,
            processing_timestamp=transformation_timestamp,
            table_name=table_name,
        )
        logger.info(
            f"Transform for {table_name} --> {new_table_name} completed with {len(df)} new records transformed."
        )

    set_current_state(final_state, LAMBDA_STATE_BUCKET_NAME, s3_client)

    logger.info("Transform process successfully ended.")

    return orjson.dumps(result)


if __name__ == "__main__":
    test_event = {
        "files_to_process": [
            {
                "table_name": "payment",
                "extraction_timestamp": "2025-06-11T14:31:49.412001",
                "last_updated": "2025-06-11T14:27:10.332000",
                "file_name": "payment_2025-6-11_14-27-10_332000.parquet",
                "key": "2025/6/11/payment_2025-6-11_14-27-10_332000.parquet",
            },
            {
                "table_name": "transaction",
                "extraction_timestamp": "2025-06-11T14:31:53.892931",
                "last_updated": "2025-06-11T14:27:10.332000",
                "file_name": "transaction_2025-6-11_14-27-10_332000.parquet",
                "key": "2025/6/11/transaction_2025-6-11_14-27-10_332000.parquet",
            },
            {
                "table_name": "design",
                "extraction_timestamp": "2025-06-11T14:31:57.612096",
                "last_updated": "2025-06-11T14:30:09.707000",
                "file_name": "design_2025-6-11_14-30-9_707000.parquet",
                "key": "2025/6/11/design_2025-6-11_14-30-9_707000.parquet",
            },
            {
                "table_name": "sales_order",
                "extraction_timestamp": "2025-06-11T14:32:01.333070",
                "last_updated": "2025-06-11T14:27:10.332000",
                "file_name": "sales_order_2025-6-11_14-27-10_332000.parquet",
                "key": "2025/6/11/sales_order_2025-6-11_14-27-10_332000.parquet",
            },
        ]
    }

    result = lambda_handler(test_event, {})
    # print(result)

    # ! these are the tables we are still missing fro post mvp
    # purchase_order
    # payment_type
    # payment
    # transaction
