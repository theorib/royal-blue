import os

import pg8000.dbapi
from dotenv import load_dotenv

load_dotenv()

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
database = os.getenv("DB_DATABASE")
port = os.getenv("DB_PORT")


def connect_db():
    try:
        conn = pg8000.dbapi.Connection(
            database=database, user=user, password=password, host=host, port=port
        )

        return conn

    except Exception as e:
        return {
            "error": {
                "message": f"Error {e}",
            }
        }


def close_db(conn: pg8000.dbapi.Connection):
    try:
        if conn:
            conn.close()
            print("CONNECTION CLOSED")
    except Exception as e:
        return {
            "error": {
                "message": f"Error {e}",
            }
        }
