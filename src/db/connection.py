import logging
import os
from typing import Any

from psycopg import Connection, Error, connect
from psycopg.rows import DictRow, dict_row

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def connect_db() -> Connection[DictRow]:
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    dbname = os.getenv("DB_DATABASE")
    port = os.getenv("DB_PORT")
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
    try:
        if conn:
            conn.close()
    except Error as error:
        logger.error(error)
        raise error
    except Exception as error:
        logger.error(error)
        raise error
