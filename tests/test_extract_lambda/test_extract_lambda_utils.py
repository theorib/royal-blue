from copy import deepcopy
from datetime import datetime

import pandas as pd
import pytest

from src.lambdas.extract_lambda.extract_lambda_utils import (
    create_data_frame_from_list,
    get_last_updated_from_raw_table_data,
)


@pytest.mark.describe("Test create_data_frame_from_list")
class TestCreateDataFramFromList:
    # @pytest.mark.skip
    @pytest.mark.it(
        "check for purity, making sure it returns a different object than the one passed as argument"
    )
    def test_purity(self):
        test_list = []
        result = create_data_frame_from_list(test_list)
        assert result is not test_list

    # @pytest.mark.skip
    @pytest.mark.it("check for mutation")
    def test_mutation(self):
        test_table_data = [
            {
                "currency_id": 1,
                "currency_code": "usd",
                "last_updated": datetime(2025, 5, 5, 5, 5, 5, 5),
            }
        ]
        test_table_data_value_ref = deepcopy(test_table_data)

        create_data_frame_from_list(test_table_data)
        assert test_table_data == test_table_data_value_ref

    # @pytest.mark.skip
    @pytest.mark.it("check that it returns None if empty list is passed")
    def test_empty_list(self):
        test_list = []
        result = create_data_frame_from_list(test_list)
        assert result is None

    # @pytest.mark.skip
    @pytest.mark.it(
        "check that it returns a dataframe if passed a list of dictionaries"
    )
    def test_success(self):
        last_updated = datetime(2025, 5, 5, 5, 5, 5, 5)

        test_table_data = [
            {
                "currency_id": 1,
                "currency_code": "usd",
                "last_updated": last_updated,
            }
        ]

        result = create_data_frame_from_list(test_table_data)
        assert isinstance(result, pd.DataFrame)

    # @pytest.mark.skip
    @pytest.mark.it("check that if passed a non iterable value it raises an exception")
    def test_(self):
        test_data = 100
        with pytest.raises(Exception):
            create_data_frame_from_list(test_data)  # type: ignore

    # @pytest.mark.skip
    @pytest.mark.it(
        "check that the dataframe has as many entries as the list passed to it"
    )
    def test_len_dataframe(self):
        test_table_data = [
            {
                "currency_id": 1,
                "currency_code": "usd",
                "last_updated": datetime(2025, 5, 5, 5, 5, 5, 5),
            },
            {
                "currency_id": 2,
                "currency_code": "eur",
                "last_updated": datetime(2025, 5, 5, 5, 0, 0, 0),
            },
        ]

        result = create_data_frame_from_list(test_table_data)
        assert len(result) == len(test_table_data)


@pytest.mark.describe("Test get_last_updated_from_raw_table_data")
class TestGetLastUpdatedFromRawTableData:
    # @pytest.mark.skip
    @pytest.mark.it("check it does not mutate list")
    def test_mutation(self):
        test_table_data = [
            {
                "currency_id": 1,
                "currency_code": "usd",
                "last_updated": datetime(2025, 5, 5, 5, 5, 5, 5),
            }
        ]
        test_table_data_value_ref = deepcopy(test_table_data)

        get_last_updated_from_raw_table_data(test_table_data)
        assert test_table_data == test_table_data_value_ref

    @pytest.mark.it("check it returns none when passed an empty list")
    def test_empty_list(self):
        test_list = []

        assert get_last_updated_from_raw_table_data(test_list) is None

    @pytest.mark.it("check it returns a datetime for list of one dictionary")
    def test_datetime_return(self):
        last_updated = datetime(2025, 5, 5, 5, 5, 5, 5)
        test_list = [
            {
                "currency_id": 0,
                "currency_code": "usd",
                "last_updated": last_updated,
            }
        ]

        assert get_last_updated_from_raw_table_data(test_list) == last_updated
        assert isinstance(get_last_updated_from_raw_table_data(test_list), datetime)

    @pytest.mark.it("check it returns the maximum datetime from a list of dictionaries")
    def test_maximum_time(self):
        test_list = [
            {
                "currency_id": 0,
                "currency_code": "usd",
                "last_updated": datetime(2025, 5, 5, 5, 5, 5, 0),
            },
            {
                "currency_id": 1,
                "currency_code": "arg",
                "last_updated": datetime(2025, 5, 5, 5, 5, 5, 3),
            },
            {
                "currency_id": 2,
                "currency_code": "brl",
                "last_updated": datetime(2025, 5, 5, 5, 5, 5, 2),
            },
            {
                "currency_id": 3,
                "currency_code": "eur",
                "last_updated": datetime(2025, 5, 5, 5, 5, 5, 1),
            },
        ]

        assert get_last_updated_from_raw_table_data(test_list) == datetime(
            2025, 5, 5, 5, 5, 5, 3
        )
