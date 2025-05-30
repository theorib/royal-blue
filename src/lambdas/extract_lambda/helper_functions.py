from pprint import pprint

import pandas as pd
from pg8000.dbapi import Connection
from pg8000.native import identifier

from src.db_conn.conn import close_db, connect_db


def execute_query(conn: Connection, query: str):
    cursor = conn.cursor()
    cursor.execute(query)

    rows = cursor.fetchall()
    column_names = [col[0] for col in cursor.description or []]
    return (rows, column_names)


def get_table_names(conn: Connection):
    query = """
        SELECT table_name
          FROM information_schema.tables
         WHERE table_schema = 'public'
    """
    rows, _ = execute_query(conn, query)
    table_names = [r[0] for r in rows if not r[0].startswith("_")]

    return table_names


# identifier, literal
def get_table_last_updated_timestamp(conn: Connection, table_name: str):
    query = f"""
        SELECT MAX(last_updated) as last_updated FROM public.{table_name}
    """
    rows, column_names = execute_query(conn, query)

    last_updated = rows[0][0]

    return last_updated


# This is where we will need to add more options to query the database
def create_dataframe_from_table(conn: Connection, table_name: str):
    sanitized_table_name = identifier(table_name)
    query = f"SELECT * FROM public.{sanitized_table_name}"
    rows, columns = execute_query(conn, query)
    return pd.DataFrame(rows, columns=columns)


if __name__ == "__main__":
    try:
        conn = connect_db()
        # print(extract_all_tables_as_dataframes())
        pprint(create_dataframe_from_table(conn, "department"))

    except Exception as err:
        print("err", err)

    finally:
        close_db(conn)
