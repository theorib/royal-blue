import pandas as pd
import pytest

from src.utilities.facts.fact_sales_order_transform import get_fact_sales_order_df


@pytest.fixture
def mock_sales_order_df():
    return pd.DataFrame(
        {
            "sales_order_id": [1],
            "created_at": ["2025-06-01T10:30:00"],
            "last_updated": ["2025-06-02T15:45:00"],
            "design_id": [101],
            "staff_id": [501],
            "counterparty_id": [301],
            "units_sold": [10],
            "unit_price": [5.99],
            "currency_id": [1],
            "agreed_delivery_date": ["2025-06-10"],
        }
    )


@pytest.mark.describe("fact_sales_order_dataframe Transformation Function")
class TestFactSalesOrder:
    @pytest.mark.it(
        "Should return a DataFrame with the required columns when input is valid"
    )
    def test_fact_sales_order_success(self, mock_sales_order_df):
        df = {"sales_order": mock_sales_order_df}
        result = get_fact_sales_order_df(df)

        expected_columns = [
            "sales_order_id",
            "created_at",
            "last_updated",
            "design_id",
            "staff_id",
            "counterparty_id",
            "units_sold",
            "unit_price",
            "currency_id",
            "agreed_delivery_date",
        ]

        assert not result.empty
        assert len(result) == 1
        for column in expected_columns:
            assert column in result

    @pytest.mark.it(
        "Should raise ValueError if 'sales_order' table is missing from input"
    )
    def test_fact_sales_order_missing_table(self):
        df = {}

        with pytest.raises(
            ValueError, match="Missing 'sales_order' table from extracted data."
        ):
            get_fact_sales_order_df(df)

    @pytest.mark.it(
        "Should raise ValueError if required columns are missing from 'sales_order'"
    )
    def test_fact_sales_order_missing_column(self):
        broken_sales_order_df = pd.DataFrame(
            {
                "sales_order_id": [1],
                "design_id": [101],
                "staff_id": [501],
            }
        )

        broken_df = {"sales_order": broken_sales_order_df}

        with pytest.raises(ValueError, match="Missing columns in 'sales_order'"):
            get_fact_sales_order_df(broken_df)
