import pandas as pd


def execute_query(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall(), cursor.description


def get_table_names(conn):
    sql = """
        SELECT table_name
          FROM information_schema.tables
         WHERE table_schema = 'public'
    """
    rows, _ = execute_query(conn, sql)
    return [r[0] for r in rows if not r[0].startswith("_")]


def get_last_updated_timestamp(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"SELECT MAX(last_updated) FROM public.{table_name}")
    result = cursor.fetchone()
    return result[0] if result else None


def format_pg8000_output(rows, desc):
    colnames = [col[0] for col in desc]
    rows_as_lists = [list(row) for row in rows]
    return rows_as_lists, colnames


def get_table_dataframe(conn, table_name):
    sql = f"SELECT * FROM public.{table_name}"
    rows, desc = execute_query(conn, sql)
    rows_as_lists, columns = format_pg8000_output(rows, desc)
    return pd.DataFrame(rows_as_lists, columns=columns)
