import logging
from unittest.mock import patch

import pytest
from psycopg import Connection, OperationalError
from psycopg.rows import dict_row

from src.db.connection import close_db, connect_db


@pytest.mark.describe("Tests connect_db")
class TestConnectDb:
    # @pytest.mark.skip
    @pytest.mark.it("check that it returns a Connection object")
    def test_return_value(self, patched_connect):
        conn = connect_db()
        assert isinstance(conn, Connection)

    # @pytest.mark.skip
    @pytest.mark.it("check that it is called once with expected arguments")
    def test_connect_success(self, patched_connect):
        conn = connect_db()

        assert conn is not None
        patched_connect.assert_called_once_with(
            "user=user password=pass host=localhost dbname=testdb port=5432",
            row_factory=dict_row,
        )

    # @pytest.mark.skip
    @pytest.mark.it("check that the DB connection is open")
    def test_connection_open(self, patched_connect):
        conn = connect_db()
        assert conn.closed == False

    # @pytest.mark.skip
    @pytest.mark.it("check that it raises an exception when connection fails")
    def test_connect_db_exception(self, patched_envs):
        error_message = "Connection failed"
        with patch("src.db.connection.connect", side_effect=Exception(error_message)):
            with pytest.raises(Exception, match=error_message):
                connect_db()

    # @pytest.mark.skip
    @pytest.mark.it(
        "check that it raises OperationalError upon entering wrong DB credentials"
    )
    def test_raises_operational_error(self, patched_envs_error):
        with pytest.raises(OperationalError) as error:
            connect_db()
        assert (
            str(error.value) == "[Errno 8] nodename nor servname provided, or not known"
        )

    # @pytest.mark.skip
    @pytest.mark.it("check that if it raises exceptions, they are logged by logger")
    def test_logs_errors(self, patched_envs, caplog):
        error_massage = "Connection failed"
        with patch("src.db.connection.connect", side_effect=Exception(error_massage)):
            with caplog.at_level(logging.ERROR) as log:
                with pytest.raises(Exception):
                    connect_db()
            logged_record = caplog.records[0]
            assert logged_record.levelname == "ERROR"
            assert error_massage in logged_record.message


@pytest.mark.describe("Tests close_db")
class TestCloseDb:
    # @pytest.mark.skip
    @pytest.mark.it("check that it closes the DB connection")
    def test_closes_db(self, patched_connect):
        conn = connect_db()
        assert conn.closed == False
        close_db(conn)
        assert conn.closed == True

    # @pytest.mark.skip
    @pytest.mark.it(
        "check that if the connection is closed multiple times there are no exceptions"
    )
    def test_close_multiple(self, patched_connect):
        conn = connect_db()
        close_db(conn)
        assert conn.closed == True
        close_db(conn)
        assert conn.closed == True

    # @pytest.mark.skip
    @pytest.mark.it("check that if any exceptions are raised they are logged by logger")
    def test_qwe(self, patched_envs, caplog):
        error_message = "Error Closing DB"
        with patch("src.db.connection.connect") as mock_connect:
            mock_connect.return_value.close.side_effect = Exception(error_message)
            conn = connect_db()

            with caplog.at_level(logging.ERROR) as log:
                with pytest.raises(Exception, match=error_message) as error:
                    close_db(conn)
            logged_record = caplog.records[0]
            assert logged_record.levelname == "ERROR"
            assert error_message in logged_record.message
