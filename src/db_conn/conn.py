import os

import pg8000.dbapi
from dotenv import load_dotenv

load_dotenv()


def connect_db():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    database = os.getenv("DB_DATABASE")
    port = os.getenv("DB_PORT")

    if not all([user, password, host, database, port]):
        return {
            "error": {
                "message": "Missing required environment variable for database connection"
            }
        }

    try:
        conn = pg8000.dbapi.Connection(
            user=user, password=password, host=host, port=int(port), database=database
        )
        return {
            "success": {
                "message": "Database connection established successfully",
                "data": conn,
            }
        }

    except Exception as e:
        return {"error": {"message": f"ERROR: {e}"}}


def close_db(conn: pg8000.dbapi.Connection):
    if conn is None:
        return {"error": {"message": "No active connection provided to close"}}

    try:
        conn.close()
        return {"success": {"message": "Database connection closed successfully"}}
    except Exception as e:
        return {"error": {"message": f"ERROR: {e}"}}
