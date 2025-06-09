from io import BytesIO

import pandas as pd


def create_data_frame_from_parquet(parquet_file):
    """
    Converts a Parquet file in bytes to a Pandas DataFrame.
    """
    try:
        restored_data_frame = pd.read_parquet(BytesIO(parquet_file))
        return restored_data_frame
    except Exception as e:
        raise ValueError(f"Failed to convert Parquet file to DataFrame: {e}")
