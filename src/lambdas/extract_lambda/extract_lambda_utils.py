import logging
from copy import deepcopy
from datetime import datetime
from typing import List

import pandas as pd

from src.utilities.custom_errors import InvalidEmptyList

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_data_frame_from_list(data: List[dict]) -> pd.DataFrame:
    """
    Convert a list of dictionaries into a pandas DataFrame.

    Args:
        data (List[dict]): A list of dictionaries representing table rows.

    Returns:
        pd.DataFrame: A DataFrame created from the input list.

    Raises:
        InvalidEmptyList: If the input list is empty.
    """
    if len(data):
        return pd.DataFrame(data)
    else:
        raise InvalidEmptyList("ERROR: List is empty")


def get_last_updated_from_raw_table_data(rows: List[dict]) -> datetime:
    """
    Extract the latest datetime from the 'last_updated' field of a list of records.

    Args:
        rows (List[dict]): A list of dictionaries, each representing a row of data.

    Returns:
        datetime: The most recent datetime found in the 'last_updated' field.

    Raises:
        InvalidEmptyList: If the input list is empty.
    """
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
    """
    Initialize the state for a table if it does not already exist in the current ingest state.

    Args:
        current_state (dict): The current ingest state dictionary.
        table_name (str): The name of the table to initialize.

    Returns:
        dict: A new state dictionary with the table initialized if it wasn't already present.
    """
    if current_state["ingest_state"].get(table_name):
        return current_state

    new_state = deepcopy(current_state)
    new_state["ingest_state"][table_name] = {
        "last_updated": None,
        "ingest_log": [],
    }
    return new_state


def create_parquet_metadata(
    new_table_data_last_updated: datetime, table_name: str
) -> tuple[str, str]:
    """
    Generate a filename and storage key for a parquet file based on timestamp and table name.

    Args:
        new_table_data_last_updated (datetime): The datetime indicating when the table data was last updated.
        table_name (str): The name of the table.

    Returns:
        tuple[str, str]: A tuple containing the parquet filename and the storage key.
    """
    year = new_table_data_last_updated.year
    month = new_table_data_last_updated.month
    day = new_table_data_last_updated.day

    # currency_2025-06-13_10-35-20_012023.parquet
    filename = f"{table_name}_{year}-{month}-{day}_{new_table_data_last_updated.hour}-{new_table_data_last_updated.minute}-{new_table_data_last_updated.second}_{new_table_data_last_updated.microsecond}.parquet"

    # 2025/06/13/currency_2025-06-13_10-35-20_012023.parquet
    key = f"{year}/{month}/{day}/{filename}"

    return filename, key
