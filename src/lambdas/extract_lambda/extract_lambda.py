import os
from pprint import pprint
from typing import List

import boto3

from src.db.connection import connect_db
from src.db.db_helpers import get_table_data, get_totesys_table_names
from src.lambdas.extract_lambda.extract_lambda_utils import (
    create_data_frame_from_list,
    get_last_updated_from_raw_table_data,
)
from src.typing_utils import EmptyDict
from src.utils import add_to_s3_bucket, create_parquets_from_data_frames

# INGEST_ZONE_BUCKET_NAME = os.environ.get("INGEST_ZONE_BUCKET_NAME")
INGEST_ZONE_BUCKET_NAME = "ingestion-zone-20250530151335299400000005"


def lambda_handler(event: EmptyDict, context: EmptyDict):
    conn = connect_db()
    s3_client = boto3.client("s3")

    with conn:
        totesys_tables = get_totesys_table_names(conn)

        all_tables_data = []

        for table_name in totesys_tables:
            response = get_table_data(conn, table_name)

            if response["success"]:
                raw_table_data = response["success"]["data"]

                table_data = {
                    "table_name": table_name,
                    "last_updated": get_last_updated_from_raw_table_data(
                        raw_table_data
                    ),
                    "data_frame": create_data_frame_from_list(raw_table_data),
                }

                all_tables_data.append(table_data)

        create_parquets_response = create_parquets_from_data_frames(all_tables_data)
        all_parquets = []

        if create_parquets_response["success"]:
            all_parquets: List[dict] = create_parquets_response["success"]["data"]  # type: ignore

        dummy = []

        for parquet in all_parquets:
            table_name, last_updated, parquet_file = parquet.values()
            year = last_updated.year
            month = last_updated.month
            day = last_updated.day
            filename = f"{table_name}_{year}-{month}-{day}_{last_updated.hour}-{last_updated.minute}-{last_updated.second}_{last_updated.microsecond}.parquet"
            key = f"{year}/{month}/{day}/{filename}"

            dummy.append({
                "table_name": table_name,
                "year": year,
                "month": month,
                "day": day,
                "filename": filename,
                "key": key,
            })

            response = add_to_s3_bucket(
                s3_client, INGEST_ZONE_BUCKET_NAME, key, parquet_file
            )

            pprint(response)


if __name__ == "__main__":
    result = lambda_handler({}, {})
    pprint(result)
