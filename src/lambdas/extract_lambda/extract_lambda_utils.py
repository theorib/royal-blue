import logging
from datetime import datetime
from typing import List, Optional

import pandas as pd

from src.lambdas.extract_lambda.custom_errors import InvalidList

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_data_frame_from_list(data: List[dict]) -> pd.DataFrame:
    if len(data):
        return pd.DataFrame(data)
    else:
        raise InvalidList("ERROR: List is empty")


def get_last_updated_from_raw_table_data(rows: List[dict]) -> datetime:
    if not len(rows):
        raise InvalidList("ERROR: List is empty")
    last_updated_column = "last_updated"
    list_of_datetimes = []
    for row in rows:
        if last_updated_column in row:
            value = row[last_updated_column]
            if isinstance(value, datetime):
                list_of_datetimes.append(value)

    return max(list_of_datetimes)
