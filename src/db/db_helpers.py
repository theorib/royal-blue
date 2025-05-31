import logging
from pprint import pprint

from psycopg import Connection, sql

from src.db.connection import close_db, connect_db

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def execute_query(conn: Connection, query):
    cursor = conn.cursor()
    cursor.execute(query)

    response = cursor.fetchall()

    return response


def get_table_names(conn: Connection):
    query = """
        SELECT table_name
          FROM information_schema.tables
         WHERE table_schema = 'public'
    """
    response = execute_query(conn, query)
    table_names = [
        item["table_name"]
        for item in response
        if not item["table_name"].startswith("_")
    ]

    return table_names


def get_table_last_updated_timestamp(conn: Connection, table_name: str):
    query = sql.SQL("SELECT MAX(last_updated) as last_updated FROM public.{}").format(
        sql.Identifier(table_name)
    )
    response = execute_query(conn, query)
    last_updated = response[0]["last_updated"]

    result = {
        "success": {
            "data": {
                "table_name": table_name,
                "last_updated": last_updated,
            }
        }
    }

    return result


def get_table_data(conn: Connection, table_name: str):
    query = sql.SQL("SELECT * FROM public.{}").format(sql.Identifier(table_name))
    rows, columns = execute_query(conn, query)

    return rows, columns


if __name__ == "__main__":
    try:
        conn = connect_db()
        query = sql.SQL("SELECT * FROM public.currency")
        #     query = """
        #     SELECT table_name
        #       FROM information_schema.tables
        #      WHERE table_schema = 'public'
        # """
        # pprint(execute_query(conn, query))
        # pprint(get_table_names(conn))
        # pprint(get_table_data(conn, "currency"))
        # print(extract_all_tables_as_dataframes())
        # pprint(create_dataframe_from_table(conn, "currency"))
        print(get_table_last_updated_timestamp(conn, "currency"))

    except Exception as err:
        print("err", err)

    finally:
        close_db(conn)
