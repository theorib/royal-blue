from io import BytesIO

import pandas as pd

from src.lambdas.extract_lambda.custom_errors import InvalidDataFrame


def create_parquet_from_data_frame(data_frame: pd.DataFrame):
    if not isinstance(data_frame, pd.DataFrame) or data_frame.empty:
        raise InvalidDataFrame("ERROR: invalid DataFrame")
    parquet_file: BytesIO = BytesIO()
    data_frame.to_parquet(parquet_file, engine="pyarrow", compression="gzip")
    parquet_file.seek(0)

    return parquet_file
