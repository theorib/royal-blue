from unittest.mock import Mock, patch

import pytest

from src.db_conn.conn import connect_db


@pytest.mark.describe("Tests the connections validity")
class TestConnection:
    @pytest.mark.it(
        "should return a valid connection object when all environment variables are set"
    )
    def test_connect_success(self, set_testdb_env):
        with patch("pg8000.dbapi.Connection") as mock_conn:
            mock_conn.return_value = Mock()
            conn = connect_db()
            assert conn is not None
            mock_conn.assert_called_once_with(
                user="user",
                password="pass",
                host="localhost",
                port=5432,
                database="testdb",
            )

    @pytest.mark.it(
        "should return an error string if the connection raises a generic exception"
    )
    def test_connect_db_exception(self, set_testdb_env):
        with patch("pg8000.dbapi.Connection", side_effect=Exception()) as mock_conn:
            result = connect_db()
            assert "ERROR" in str(result)
            mock_conn.assert_called_once()

    @pytest.mark.it(
        "should return a missing environment variable error if DB_USER is missing"
    )
    def test_connect_missing_env_var(self, set_testdb_env, monkeypatch):
        monkeypatch.delenv("DB_USER", raising=False)
        with patch("pg8000.dbapi.Connection") as mock_conn:
            result = connect_db()
            assert "Missing required environment variable" in result["error"]["message"]
            mock_conn.assert_not_called()