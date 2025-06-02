import logging
from datetime import datetime
from typing import List, Optional

import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_data_frame_from_list(data: List[dict]) -> Optional[pd.DataFrame]:
    if len(data):
        return pd.DataFrame(data)
    else:
        return None


def get_last_updated_from_raw_table_data(rows: List[dict]) -> Optional[datetime]:
    if not rows:
        return None

    last_updated_column = "last_updated"
    list_of_datetimes = []

    # [
    #     row.get("last_updated")
    #     for row in rows
    #     if isinstance(row.get("last_updated"), datetime)
    # ]
    for row in rows:
        if last_updated_column in row:
            value = row[last_updated_column]
            if isinstance(value, datetime):
                list_of_datetimes.append(value)

    return max(list_of_datetimes)
