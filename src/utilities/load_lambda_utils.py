import logging

import pandas as pd
from psycopg import Connection, sql
from psycopg.rows import DictRow, tuple_row

from src.db.connection import connect_db

logger = logging.getLogger(__name__)


def create_db_entries_from_df(conn: Connection[DictRow], table_name, df: pd.DataFrame):
    with conn.cursor(row_factory=tuple_row) as cursor:
        if df.empty:
            logger.info(f"No entries available for {table_name}.")
            return

        columns = list(df.columns)

        insert_query = sql.SQL("""
            INSERT INTO {table_name} ({columns})
            VALUES ({values})
        """).format(
            table_name=sql.Identifier(table_name),
            columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
            values=sql.SQL(", ").join(sql.Placeholder() * len(columns)),
        )

        values_to_insert = df.to_records(index=False).tolist()

        try:
            cursor.executemany(insert_query, values_to_insert)

            conn.commit()

            logger.info(f"Inserted {len(values_to_insert)} rows into {table_name}.")

        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to insert records into {table_name}: {str(e)}")


if __name__ == "__main__":

    def dim_currency_dataframe(data_frame: pd.DataFrame):
        currency_lookup = {
            "USD": "US Dollar",
            "EUR": "Euro",
            "GBP": "British Pound",
            "JPY": "Japanese Yen",
            "AUD": "Australian Dollar",
            "CAD": "Canadian Dollar",
            "CHF": "Swiss Franc",
            "CNY": "Chinese Yuan",
            "NZD": "New Zealand Dollar",
            "SEK": "Swedish Krona",
            "NOK": "Norwegian Krone",
            "MXN": "Mexican Peso",
            "INR": "Indian Rupee",
            "BRL": "Brazilian Real",
            "ZAR": "South African Rand",
            "SGD": "Singapore Dollar",
            "HKD": "Hong Kong Dollar",
            "KRW": "South Korean Won",
            "RUB": "Russian Ruble",
            "TRY": "Turkish Lira",
        }
        currency_df = data_frame
        if currency_df is None:
            raise ValueError("Error: 'currency' DataFrame is missing.")

        lookup_df = (
            pd.DataFrame.from_dict(
                currency_lookup, orient="index", columns=["currency_name"]
            )
            .reset_index()
            .rename(columns={"index": "currency_code"})
        )

        currency_df = currency_df.merge(
            lookup_df[["currency_code", "currency_name"]],
            how="left",
            on="currency_code",
        )

        dim_currency = currency_df[["currency_id", "currency_code", "currency_name"]]
        return dim_currency

    conn = connect_db("DATAWAREHOUSE")
    with conn:
        currency_df = pd.read_parquet(
            "./notebook/test_data/currency_2022-11-3_14-20-49_962000.parquet"
        )
        # print("currency_df", currency_df)

        dim_currency_df = dim_currency_dataframe(currency_df)
        # print(dim_currency_df)
        create_db_entries_from_df(conn, "dim_currency", dim_currency_df)
