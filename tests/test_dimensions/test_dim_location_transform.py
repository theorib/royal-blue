import pandas as pd
import pytest

from src.lambdas.transform_lambda.dimensions.dim_location_transform import (
    dim_location_dataframe,
)


@pytest.fixture
def mock_address_df():
    return pd.DataFrame(
        {
            'address_id': ["1", "25"],
            'address_line_1': ["123 Lane", "456 Avenue"],
            'address_line_2': ["Suite 4", "Flat 5"],
            'district': ["Central", "North"],
            'city': ["London", "Bristol"],
            'postal_code': ["E1 6AN", "BS1 4TB"],
            'country': ["UK", "UK"],
            'phone': ["0123456789", "0123456774"],
        }
    )

@pytest.mark.describe("dim_location_dataframe Function Behaviour")
class TestDimLocationDataframe:

    @pytest.mark.it("Should return a dataframe with the correct columns")
    def test_dim_address_in_DF(self, mock_address_df):

        test_extracted_dataframes = {"address": mock_address_df}
        result = dim_location_dataframe(test_extracted_dataframes)

        assert isinstance(result, pd.DataFrame)

        columns = ['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']
        for title in columns:
            assert title in result.columns

        assert len(result) == 2

# @pytest.mark.it("Should raise ValueError if the address table is missing")
#     def test_dim_address_missing_address_table(self, mock_Address_df):
#     pass


# def test_dim_address_empty_DF():
#     pass

# def test_dim_address_partial_address_entry():
#     pass


# def test_dim_address_duplicates():
#     pass

# def test_dim_address_incorrect_data_type():
#     pass

# def test_dim_address_missing_columns():
#     pass


# def test_dim_address_unrelated_columns():
#     pass
