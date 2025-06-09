import logging
import os
from typing import Literal

from psycopg import Connection, Error, connect
from psycopg.rows import DictRow, dict_row

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def connect_db(db_source: Literal["TOTESYS", "DATAWAREHOUSE"]) -> Connection[DictRow]:
    """
    Connects to either the TOTESYS or DATAWAREHOUSE database using environment variables.
    Returns a psycopg connection with rows as dictionaries.
    
    Raises a ValueError if the db_source is invalid.
    """
    if db_source not in ["TOTESYS", "DATAWAREHOUSE"]:
        raise ValueError(
            "db_source invalid, must be either 'TOTESYS' or 'DATAWAREHOUSE'"
        )

    user = os.getenv("f{db_source}_DB_USER")
    password = os.getenv("f{db_source}_PASSWORD")
    host = os.getenv("f{db_source}_HOST")
    dbname = os.getenv("f{db_source}_DATABASE")
    port = os.getenv("f{db_source}_PORT")

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
