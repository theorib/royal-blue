from src.db_conn.conn import close_db, connect_db
from src.lambdas.extract_lambda.helper_functions import (
    create_dataframe_from_table,
    get_table_last_updated_timestamp,
    get_table_names,
)


def extract_all_tables_as_dataframes():
    conn = connect_db()

    try:
        dataframes = []
        for table_name in get_table_names(conn):
            table_df = create_dataframe_from_table(conn, table_name)
            last_updated = get_table_last_updated_timestamp(conn, table_name)

            result = {
                "table_name": table_name,
                "last_updated": last_updated,
                "data_frame": table_df,
            }
            dataframes.append(result)

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


if __name__ == "__main__":
    print(extract_all_tables_as_dataframes())
    # extract_all_tables_as_dataframes()
