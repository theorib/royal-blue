import pandas as pd
import pytest

from src.lambdas.transform_lambda.dimensions.dim_currency_transform import (
    dim_currency_dataframe,
)


@pytest.fixture
def valid_currency_dataframe():
    """
    Returns a dictionary containing a single DataFrame under the key "currency",
    which can be passed to dim_currency_dataframe for testing.
    """
    currency_df = pd.DataFrame(
        {
            "currency_id": [1, 2, 3],
            "currency_code": ["USD", "EUR", "JPY"],
        }
    )
    return {"currency": currency_df}


@pytest.mark.describe("tests the functionality of the currency dimension function")
class TestCurrencyDimensions:
    @pytest.mark.it("check should return a DataFrame")
    def test_transform_returns_dataframe(self, valid_currency_dataframe):
        result = dim_currency_dataframe(valid_currency_dataframe)
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.it("check should test that column names match OLAP data")
    def test_columns(self, valid_currency_dataframe):
        result = dim_currency_dataframe(valid_currency_dataframe)
        expected = ["currency_id", "currency_code", "currency_name"]

        assert list(result.columns) == expected

    @pytest.mark.it("should return correct currency names after merging lookup")
    def test_currency_lookup_merge(self, valid_currency_dataframe):
        result = dim_currency_dataframe(valid_currency_dataframe)

        assert result.shape == (3, 3)
        assert list(result.columns) == ["currency_id", "currency_code", "currency_name"]

        expected_names = {"USD": "US Dollar", "EUR": "Euro", "JPY": "Japanese Yen"}

        for _, row in result.iterrows():
            code = row["currency_code"]
            assert row["currency_name"] == expected_names[code]
