import pandas as pd
import pytest

# from pandas.testing import assert_frame_equal
from src.lambdas.transform_lambda.utils.convert_dataframes_to_dimensions import (
    dim_staff_dataframe,
)


@pytest.fixture
def oltp_staff():
    return {
        "staff": pd.DataFrame({
            'staff_id': [1, 2],
            'first_name': ['Oliver', 'San'],
            'last_name': ['kwa', 'fran'],
            'department_id': [5, 9],
            'email_address': ['oli@example.com', 's@example.com'],
            'created_at': pd.to_datetime(['2025-01-01 10:00:00', '2025-01-02 12:30:00']),
            'last_updated': pd.to_datetime(['2025-01-01 10:00:00', '2025-01-02 12:30:00'])
    })}

class TestConvertDataframesToDimensions:
    @pytest.mark.it("check should return a DataFrame")
    def test_transform_returns_dataframe(oltp_staff):
        result = dim_staff_dataframe(oltp_staff)
        assert isinstance(result, pd.DataFrame)
    
    @pytest.mark.it('check should')
    def test_1(self, oltp_staff):
        result = dim_staff_dataframe(oltp_staff)
        expected_columns = {
            'staff_id',
            'first_name',
            'last_name',
            'department_name',
            'location',
            'email_address'
        }
        assert result == expected_columns 