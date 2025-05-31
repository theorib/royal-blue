import logging
import os

from dotenv import load_dotenv
from psycopg import Connection, Error, connect
from psycopg.rows import dict_row

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def connect_db():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    dbname = os.getenv("DB_DATABASE")
    port = os.getenv("DB_PORT")
    conn = None
    try:
        conn = connect(
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


def close_db(conn: Connection):
    try:
        if conn:
            conn.close()
    except Error as error:
        logger.error(error)
        raise error
    except Exception as error:
        logger.error(error)
        raise error
