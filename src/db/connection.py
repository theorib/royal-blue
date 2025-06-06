import logging
import os
from typing import Any

from psycopg import Connection, Error, connect
from psycopg.rows import DictRow, dict_row

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def connect_db() -> Connection[DictRow]:
    """
    Establish a connection to a PostgreSQL database using environment variables.

    Environment Variables:
        DB_USER (str): Database username.
        DB_PASSWORD (str): Database password.
        DB_HOST (str): Hostname of the database.
        DB_DATABASE (str): Name of the database.
        DB_PORT (str): Port number (optional, defaults to 0000 if not set).

    Returns:
        Connection[DictRow]: A psycopg connection object with dictionary row factory enabled.

    Raises:
        psycopg.Error: If a database connection error occurs.
        Exception: For any other unexpected exceptions during connection.
    """
    user = os.getenv("TOTESYS_DB_USER")
    password = os.getenv("TOTESYS_DB_PASSWORD")
    host = os.getenv("TOTESYS_DB_HOST")
    dbname = os.getenv("TOTESYS_DB_DATABASE")
    port = os.getenv("TOTESYS_DB_PORT")
    try:
        conn: Connection[DictRow] = connect(
            f"user={user} password={password} host={host} dbname={dbname} port={int(port or 0000)}",
            row_factory=dict_row,  # type: ignore
        )

        return conn
    except Error as error:
        logger.error(error)
        raise error
    except Exception as error:
        logger.error(error)
        raise error


def close_db(conn: Connection[Any]) -> None:
    """
    Safely close a PostgreSQL database connection.

    Args:
        conn (Connection[Any]): A psycopg connection object.

    Raises:
        psycopg.Error: If an error occurs while closing the connection.
        Exception: For any other unexpected exceptions.
    """
    try:
        if conn:
            conn.close()
    except Error as error:
        logger.error(error)
        raise error
    except Exception as error:
        logger.error(error)
        raise error
