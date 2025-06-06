from io import BytesIO

import pandas as pd


def parquet_to_dataframe(parquet_file):
    """
    Converts a Parquet file in bytes to a Pandas DataFrame.
    """
    try:
        restored_data_frame = pd.read_parquet(BytesIO(parquet_file))
        return restored_data_frame
    except Exception as e:
        raise ValueError(f"Failed to convert Parquet file to DataFrame: {e}")
