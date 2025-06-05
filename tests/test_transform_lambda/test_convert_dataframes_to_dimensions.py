import pandas as pd
import pytest

# from pandas.testing import assert_frame_equal
from src.lambdas.transform_lambda.dimensions.dim_staff_transform import (
    dim_staff_dataframe,
)


@pytest.fixture(scope="function")
def test_extracted_dataframe():
    return {
        "staff": pd.DataFrame(
            {
                "staff_id": [1, 2],
                "first_name": ["Oliver", "San"],
                "last_name": ["kwa", "fran"],
                "department_id": [5, 9],
                "email_address": ["oli@example.com", "s@example.com"],
                "created_at": pd.to_datetime(
                    ["2025-01-01 10:00:00", "2025-01-02 12:30:00"]
                ),
                "last_updated": pd.to_datetime(
                    ["2025-01-01 10:00:00", "2025-01-02 12:30:00"]
                ),
            }
        ),
        "department": pd.DataFrame(
            {"department_id": [5, 9], "department_name": ["marketing", "sales"]}
        ),
        "address": pd.DataFrame(
            {
                "address_id": [101, 102],
                "address_line_1": ["123 Main St", "456 High St"],
                "address_line_2": ["Suite A", "Floor 3"],
                "district": ["Central", "North"],
                "city": ["London", "Manchester"],
                "postal_code": ["SW1A 1AA", "M1 1AA"],
                "country": ["UK", "UK"],
                "phone": ["+441234567890", "+441987654321"],
                "created_at": pd.to_datetime(["2025-01-01", "2025-01-02"]),
                "last_updated": pd.to_datetime(["2025-01-03", "2025-01-04"]),
            }
        ),
        "purchase_order": pd.DataFrame(
            {
                "purchase_order_id": [100, 101],
                "created_at": pd.to_datetime(
                    ["2025-02-01 09:15:00", "2025-02-02 14:30:00"]
                ),
                "last_updated": pd.to_datetime(
                    ["2025-02-01 09:15:00", "2025-02-02 14:30:00"]
                ),
                "staff_id": [1, 2],
                "counterparty_id": [3, 4],
                "item_code": ["ITEM001", "ITEM002"],
                "item_quantity": [10, 20],
                "item_unit_price": [99.99, 149.50],
                "currency_id": [1, 2],
                "agreed_delivery_date": pd.to_datetime(["2025-02-10", "2025-02-15"]),
                "agreed_payment_date": pd.to_datetime(["2025-02-05", "2025-02-20"]),
                "agreed_delivery_location_id": [101, 102],
            }
        ),
    }


class TestConvertDataframesToDimensions:
    @pytest.mark.it("check should return a DataFrame")
    def test_transform_returns_dataframe(self, test_extracted_dataframe):
        result = dim_staff_dataframe(test_extracted_dataframe)
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.it("check should")
    def test_1(self, test_extracted_dataframe):
        result = dim_staff_dataframe(test_extracted_dataframe)
        expected_columns = {
            "staff_id",
            "first_name",
            "last_name",
            "department_name",
            "location",
            "email_address",
        }

        assert set(result.columns) == expected_columns
