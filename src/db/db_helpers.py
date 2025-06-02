import logging
from datetime import datetime
from typing import List

from psycopg import Connection, sql
from psycopg.rows import DictRow

from src.db.error_map import ERROR_MAP

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

        return {
            "success": {
                "data": totesys_table_names
            }
        }

    except Exception as e:
        return handle_db_exception(e)


def get_table_last_updated_timestamp(conn: Connection[DictRow], table_name: str):
    try:
        query = sql.SQL(
            "SELECT MAX(last_updated) as last_updated FROM public.{}"
        ).format(sql.Identifier(table_name))

        with conn:
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
    try:
        base_query = sql.SQL("SELECT * FROM public.{}").format(
            sql.Identifier(table_name)
        )

        if last_updated:
            query_with_last_updated = sql.SQL("WHERE last_updated >= {}").format(
                sql.Literal(last_updated)
            )

            query = base_query + query_with_last_updated
        else:
            query = base_query

        with conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                db_response = cursor.fetchall()

        return {"success": {"data": db_response}}

    except Exception as e:
        return handle_db_exception(e)


def handle_db_exception(e: Exception) -> dict:
    error_type = type(e).__name__
    error_message = ERROR_MAP.get(type(e), ERROR_MAP[Exception])
    logger.error(f"{error_type}: {error_message} | {str(e)}")

    return {
        "error": {
            "message": f"{error_type}: {error_message}",
        }
    }
