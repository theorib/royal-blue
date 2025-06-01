import logging
from pprint import pprint
from typing import List

from psycopg import Connection, Error, sql
from psycopg.errors import ProgrammingError
from psycopg.rows import DictRow

from src.db.connection import close_db, connect_db

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def filter_out_values(values: List[str], values_to_filter: List[str]):
    filtered = [value for value in values if value not in values_to_filter]
    return filtered


def get_totesys_table_names(
    conn: Connection[DictRow],
    table_names_to_filter_out: List[str] = ["_prisma_migrations"],
):
    query = """
        SELECT table_name
          FROM information_schema.tables
         WHERE table_schema = 'public'
    """

    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                response = cursor.fetchall()
                logger.debug(response)

                totesys_table_names = [
                    item["table_name"]
                    for item in response
                    # filter out tables that should not be
                    if item["table_name"] not in table_names_to_filter_out
                ]
                logger.debug(totesys_table_names)

        return totesys_table_names

    except Exception as e:
        logger.error(e)
        raise e


def get_table_last_updated_timestamp(conn: Connection[DictRow], table_name: str):
    try:
        query = sql.SQL(
            "SELECT MAX(last_updated) as last_updated FROM public.{}"
        ).format(sql.Identifier(table_name))

        with conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                response = cursor.fetchone()
                pprint(response)

        if response:
            result = {
                "success": {
                    "data": {
                        "table_name": table_name,
                        "last_updated": response["last_updated"],
                    }
                }
            }
            return result
        return {"error": {"message": "invalid database response"}}
    except Error as error:
        print(error.)
        # error_lookup = {
        #     'UndefinedTable' =
        # }
        # logger.error(error)
        raise error


# def get_table_data(conn: Connection, table_name: str):
#     query = sql.SQL("SELECT * FROM public.{}").format(sql.Identifier(table_name))
#     rows, columns = execute_query(conn, query)

#     return rows, columns


if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)
    logging.basicConfig(level=logging.NOTSET)

    try:
        conn = connect_db()
        get_totesys_table_names(conn)
        # query = sql.SQL("SELECT * FROM public.currency")
        #     query = """
        #     SELECT table_name
        #       FROM information_schema.tables
        #      WHERE table_schema = 'public'
        # """
        # pprint(execute_query(conn, query))
        # pprint(get_totesys_table_names(conn))
        # pprint(get_table_data(conn, "currency"))
        # print(extract_all_tables_as_dataframes())
        # pprint(create_dataframe_from_table(conn, "currency"))
        # print(get_table_last_updated_timestamp(conn, "currency"))

    except Exception as err:
        logger.error(err)

    # finally:
    #     close_db(conn)
