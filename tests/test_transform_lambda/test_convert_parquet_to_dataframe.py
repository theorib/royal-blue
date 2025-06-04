from io import BytesIO

import pandas as pd
import pytest

from src.lambdas.transform_lambda.utils.convert_parquet_to_dataframe import (
    parquet_to_dataframe,
)


@pytest.fixture
def test_dataframe():
    return pd.DataFrame({
        "name": ["Charley", "San", "Oliver"],
        "favourite_icecream": ["Strawberry", "Chocolate", "Vanilla"]
    })

@pytest.mark.describe("parquet_to_dataframe Utility Function Behaviour")
class TestCreateDataFramesfromParquets:
    @pytest.mark.it("Should return a valid DataFrame from a Parquet bytes")
    def test_returns_data_in_parquet_format(self, test_dataframe):
        buffer = BytesIO()
        test_dataframe.to_parquet(buffer, index=False)
        test_dataframe_parquet = buffer.getvalue()

        result = parquet_to_dataframe(test_dataframe_parquet)

        assert isinstance(result, pd.DataFrame)