import logging
import os

from dotenv import load_dotenv
from pg8000.dbapi import Connection

logger = logging.getLogger(__name__)

load_dotenv()


def connect_db():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    database = os.getenv("DB_DATABASE")
    port = os.getenv("DB_PORT")

    conn = None
    try:
        conn = Connection(
            user=user,
            password=password,
            host=host,
            port=int(port),
            database=database,
        )
        return conn
    except Exception as e:
        logger.error(e)
        raise e


def close_db(conn: Connection):
    try:
        if conn:
            conn.close()
    except Exception as e:
        logger.error(e)
        raise e
