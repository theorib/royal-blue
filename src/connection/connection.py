import os

import pg8000.native
from dotenv import load_dotenv

load_dotenv()

def connect_db(): 
    try:
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        database = os.getenv("DB_DATABASE")
        port = os.getenv("DB_PORT")

        conn = pg8000.native.Connection(
        database=database,
        user=user,
        password=password,
        host=host,
        port=port
        )
        
        return conn
        
    except Exception as e:
        return f"ERROR: {e}"
    

def close_db(conn: pg8000.native.Connection):
    try:
        if conn:
            conn.close()
            print("CONNECTION CLOSED")
    except Exception as e:
        return f"ERROR: {e}"
