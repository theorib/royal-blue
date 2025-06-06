import logging
from datetime import datetime
from typing import List

from psycopg import Connection, sql
from psycopg.rows import DictRow

from src.db.error_map import ERROR_MAP

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def filter_out_values(values: List[str], values_to_filter: List[str]):
    """
    Remove specified values from a list.

    Args:
        values (List[str]): The original list of string values.
        values_to_filter (List[str]): Values that should be removed from the list.

    Returns:
        List[str]: A new list excluding the values in `values_to_filter`.
    """
    filtered = [value for value in values if value not in values_to_filter]
    return filtered


def get_totesys_table_names(
    conn: Connection[DictRow],
    table_names_to_filter_out: List[str] = ["_prisma_migrations"],
) -> List[str]:
    """
    Retrieve the names of all public tables in the database, excluding specified ones.

    Args:
        conn (Connection[DictRow]): A database connection object.
        table_names_to_filter_out (List[str], optional): Table names to exclude. Defaults to ['_prisma_migrations'].

    Returns:
        List[str]: A list of table names that are not filtered out.

    Raises:
        Exception: Returns a handled error dictionary if query fails.
    """
    query = """
        SELECT table_name
          FROM information_schema.tables
         WHERE table_schema = 'public'
    """

    try:
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

        return totesys_table_names

    except Exception as e:
        return handle_db_exception(e)  # type: ignore


def get_table_last_updated_timestamp(conn: Connection[DictRow], table_name: str):
    """
    Fetch the most recent 'last_updated' timestamp from a given table.

    Args:
        conn (Connection[DictRow]): A database connection object.
        table_name (str): The name of the table to query.

    Returns:
        dict: A dictionary with either:
              - 'success': containing the table name and its max last_updated value
              - 'error': if the query fails or the response is invalid
    """
    try:
        query = sql.SQL(
            "SELECT MAX(last_updated) as last_updated FROM public.{}"
        ).format(sql.Identifier(table_name))

        with conn.cursor() as cursor:
            cursor.execute(query)
            response = cursor.fetchone()

        if response and response["last_updated"] is not None:
            return {
                "success": {
                    "data": {
                        "table_name": table_name,
                        "last_updated": response["last_updated"],
                    }
                }
            }

        return {"error": {"message": "invalid database response"}}

    except Exception as e:
        return handle_db_exception(e)


def get_table_data(
    conn: Connection[DictRow], table_name: str, last_updated: datetime | None = None
):
    """
    Retrieve all data from a table, optionally filtering by 'last_updated' timestamp.

    Args:
        conn (Connection[DictRow]): A database connection object.
        table_name (str): The name of the table to query.
        last_updated (datetime | None, optional): A datetime to filter records updated after. Defaults to None.

    Returns:
        dict: A dictionary with:
              - 'success': containing the list of row data
              - 'error': if the query fails
    """
    try:
        base_query = sql.SQL("SELECT * FROM public.{}").format(
            sql.Identifier(table_name)
        )

        if last_updated:
            query_with_last_updated = sql.SQL("WHERE last_updated > {}").format(
                sql.Literal(last_updated)
            )

            query = base_query + query_with_last_updated

        else:
            query = base_query

        with conn.cursor() as cursor:
            cursor.execute(query)
            db_response = cursor.fetchall()

            result = {"success": {"data": db_response}}

        return result

    except Exception as e:
        return handle_db_exception(e)


def handle_db_exception(e: Exception) -> dict:
    """
    Handle database exceptions and return a formatted error message.

    Args:
        e (Exception): The exception that was raised.

    Returns:
        dict: A dictionary with an 'error' message describing the exception type and mapped message.
    """
    error_type = type(e).__name__
    error_message = ERROR_MAP.get(type(e), ERROR_MAP[Exception])
    logger.error(f"{error_type}: {error_message} | {str(e)}")

    return {
        "error": {
            "message": f"{error_type}: {error_message}",
        }
    }
