from conn import close_db, connect_db
from helper_functions import (
    get_last_updated_timestamp,
    get_table_dataframe,
    get_table_names,
)


def extract_all_tables_as_dataframes():
    conn_result = connect_db()
    if "error" in conn_result:
        return conn_result

    conn = conn_result["success"]["data"]

    try:
        dataframes = {}
        for tbl in get_table_names(conn):
            df = get_table_dataframe(conn, tbl)
            ts = get_last_updated_timestamp(conn, tbl)
            dataframes[tbl] = {"data": df, "last_updated": ts}

        return {
            "success": {
                "message": "Dataframes created successfully",
                "data": dataframes,
            }
        }
    except Exception as e:
        return {"error": {"message": f"ERROR: {e}"}}
    finally:
        close_db(conn)


print(extract_all_tables_as_dataframes())
