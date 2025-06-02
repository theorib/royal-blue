import logging
from copy import copy
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from psycopg import Connection

from src.db.connection import connect_db
from src.db.db_helpers import (
    filter_out_values,
    get_table_last_updated_timestamp,
    get_totesys_table_names,
)


@pytest.mark.describe("Test filter_out_values")
class TestFilterOutValues:
    # @pytest.mark.skip
    @pytest.mark.it("check for purity")
    def test_purity(self):
        test_table_names = ["a", "b", "c"]
        test_filtered_names = ["b"]

        result = filter_out_values(test_table_names, test_filtered_names)

        assert result is not test_table_names
        assert result is not test_filtered_names

    # @pytest.mark.skip
    @pytest.mark.it("check for argument mutation")
    def test_(self):
        test_table_names = ["a", "b", "c"]
        test_filtered_names = ["b"]
        test_table_names_value_ref = copy(test_table_names)
        test_filtered_names_value_ref = copy(test_filtered_names)

        filter_out_values(test_table_names, test_filtered_names)

        assert test_table_names == test_table_names_value_ref
        assert test_filtered_names == test_filtered_names_value_ref

    # @pytest.mark.skip
    @pytest.mark.it("check that if passed an empty list it returns an empty list")
    def test_empty_list(self):
        test_table_names = []
        test_filtered_names = []

        result = filter_out_values(test_table_names, test_filtered_names)
        assert result == []

    # @pytest.mark.skip
    @pytest.mark.it(
        "check that if passed a list with no filterable values, no values are filtered"
    )
    def test_no_values_to_filter(self):
        test_table_names = ["a", "b", "c"]
        test_filtered_names = ["e"]

        result = filter_out_values(test_table_names, test_filtered_names)

        assert result == test_table_names

    # @pytest.mark.skip
    @pytest.mark.it("check that it returns a list with filtered names removed")
    def test_remove_filtered(self):
        test_table_names = ["a", "b", "c"]
        test_filtered_names = ["b"]
        expected_result = ["a", "c"]

        result = filter_out_values(test_table_names, test_filtered_names)
        assert result == expected_result


@pytest.mark.describe("Test get_totesys_table_names")
class TestGetTotesysTableNames:
    # @pytest.mark.skip
    @pytest.mark.it("check that it returns a list of strings")
    def test_returns_list_str(self):
        conn = connect_db()

        result = get_totesys_table_names(conn)

        assert isinstance(result, list)
        for item in result:
            assert isinstance(item, str)

    # @pytest.mark.skip
    @pytest.mark.it("check that filtered out names are not in the returned list")
    def test_filtered_values(self):
        conn = connect_db()
        table_names_to_filter_out = ["_prisma_migrations"]

        result = get_totesys_table_names(conn, table_names_to_filter_out)

        assert (
            list(set(result).difference(table_names_to_filter_out)).sort()
            == result.sort()
        )

    # @pytest.mark.skip
    @pytest.mark.it(
        "check that if Exceptions are raised that they get logged by logger"
    )
    def test_error_logging(self, caplog):
        conn = MagicMock(spec=Connection)
        error_message = "test exception"
        conn.cursor.side_effect = Exception(error_message)
        with caplog.at_level(logging.ERROR):
            with pytest.raises(Exception, match=error_message):
                get_totesys_table_names(conn)

        logged_record = caplog.records[0]
        assert logged_record.levelname == "ERROR"
        assert error_message in logged_record.message


@pytest.mark.describe("Test get_table_last_updated_timestamp")
class TestGetTableLastUpdatedTimestamp:
    # @pytest.mark.skip
    @pytest.mark.it(
        "check that it returns the latest updated timestamp of table currency"
    )
    def test_returns_timestamp(self):
        conn = connect_db()
        table_name = "currency"

        result = get_table_last_updated_timestamp(conn, table_name)

        assert result["success"]["data"]["table_name"] == table_name  # type: ignore
        assert isinstance(result["success"]["data"]["last_updated"], datetime)  # type: ignore

    # @pytest.mark.skip
    @pytest.mark.it(
        "check it returns an error dictionary with UndefinedTable Exception if there are no records to return"
    )
    def test_(self):
        conn = connect_db()
        table_name = "concurrency"

        result = get_table_last_updated_timestamp(conn, table_name)

        assert (
            result["error"]["message"] == f"ERROR: Table {table_name} does not exist."
        )

    @pytest.mark.skip
    @pytest.mark.it(
        "check it returns an error dictionary if there are no records to return"
    )
    def test_no(self):
        pass
