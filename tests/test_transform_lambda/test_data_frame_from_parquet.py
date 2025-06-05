from io import BytesIO
from unittest.mock import Mock

import pandas as pd
import pytest

from src.lambdas.transform_lambda.utils.convert_parquet_to_dataframe import (
    parquet_to_dataframe,
)


def data_frame_from_parquet(parquet_file):
    pass
