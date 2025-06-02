import logging

import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def create_data_frame_from_dict(data: dict) -> pd.DataFrame:
    table_df = pd.DataFrame(data)
    logger.debug(table_df)
    return table_df
