import logging
from copy import deepcopy
from datetime import datetime
from typing import List

import pandas as pd

from src.utilities.custom_errors import InvalidEmptyList

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_data_frame_from_list(data: List[dict]) -> pd.DataFrame:
    if len(data):
        return pd.DataFrame(data)
    else:
        raise InvalidEmptyList("ERROR: List is empty")


def get_last_updated_from_raw_table_data(rows: List[dict]) -> datetime:
    if not len(rows):
        raise InvalidEmptyList("ERROR: List is empty")
    last_updated_column = "last_updated"
    list_of_datetimes = []
    for row in rows:
        if last_updated_column in row:
            value = row[last_updated_column]
            if isinstance(value, datetime):
                list_of_datetimes.append(value)

    return max(list_of_datetimes)


def initialize_table_state(current_state, table_name):
    if current_state["ingest_state"].get(table_name):
        return current_state

    new_state = deepcopy(current_state)
    new_state["ingest_state"][table_name] = {
            "last_updated": None,
            "ingest_log": [],
    }
    return new_state

def create_parquet_metadata(new_table_data_last_updated:datetime, table_name:str) -> tuple[str, str]:
    year = new_table_data_last_updated.year
    month = new_table_data_last_updated.month
    day = new_table_data_last_updated.day

    # currency_2025-06-13_10-35-20_012023.parquet
    filename = f"{table_name}_{year}-{month}-{day}_{new_table_data_last_updated.hour}-{new_table_data_last_updated.minute}-{new_table_data_last_updated.second}_{new_table_data_last_updated.microsecond}.parquet"

    # 2025/06/13/currency_2025-06-13_10-35-20_012023.parquet
    key = f"{year}/{month}/{day}/{filename}"

    return filename, key