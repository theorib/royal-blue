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

        values_to_insert = df.to_records(index=False).tolist()

        insert_query = sql.SQL("""
            INSERT INTO {table_name} ({columns})
            VALUES ({values})
        """).format(
            table_name=sql.Identifier(table_name),
            columns=sql.SQL(",").join(map(sql.Identifier, columns)),
            values=sql.SQL(",").join(sql.Placeholder() * len(columns)),
        )

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

    def dim_design_dataframe(data_frame: pd.DataFrame) -> pd.DataFrame:
        """
        Create the design dimension DataFrame from the extracted raw design data.

        Parameters:
        -----------
        dataframes : dict
            Dictionary containing raw DataFrames extracted from source tables.
            Must contain a 'design' DataFrame.

        Returns:
        --------
        pd.DataFrame
            A DataFrame representing the design dimension with columns:
            'design_id', 'design_name', 'file_location', 'file_name'.

        Raises:
        -------
        ValueError
            If the 'design' table is missing or any error occurs during transformation.
        """

        design_df = data_frame

        if design_df is None:
            raise ValueError("Error: Missing design table.")

        try:
            dim_design = design_df[
                ["design_id", "design_name", "file_location", "file_name"]
            ].drop_duplicates()

            return dim_design

        except Exception as e:
            raise ValueError(f"Error creating dim_design: {e}")

    def dim_date_dataframe(start_date: str, end_date: str):
        all_dates = pd.date_range(
            start=start_date, end=end_date, tz="Europe/London", normalize=True
        )
        date_dataframe = pd.DataFrame({"date_id": all_dates})

        date_dataframe["year"] = date_dataframe["date_id"].dt.year
        date_dataframe["month"] = date_dataframe["date_id"].dt.month
        date_dataframe["day"] = date_dataframe["date_id"].dt.day
        date_dataframe["day_of_week"] = date_dataframe["date_id"].dt.weekday + 1
        date_dataframe["day_name"] = date_dataframe["date_id"].dt.day_name()
        date_dataframe["month_name"] = date_dataframe["date_id"].dt.month_name()
        date_dataframe["quarter"] = date_dataframe["date_id"].dt.quarter
        date_dataframe["date_id"] = date_dataframe["date_id"].dt.date

        date_dataframe = date_dataframe[
            [
                "date_id",
                "year",
                "month",
                "day",
                "day_of_week",
                "day_name",
                "month_name",
                "quarter",
            ]
        ]

        return date_dataframe

    currency_df = pd.read_parquet(
        "./sql_local_tests/seed_data/currency_2022-11-3_14-20-49_962000.parquet"
    )
    dim_currency_df = dim_currency_dataframe(currency_df)
    design_df = pd.read_parquet(
        "./sql_local_tests/seed_data/design_2025-6-9_12-0-9_793000.parquet"
    )
    dim_design_df = dim_design_dataframe(design_df)
    # dim_date_df = dim_date_dataframe("20221102", "20500101")
    dim_date_df = dim_date_dataframe(
        "2022-11-01 00:00:00.000000", "2050-11-01 00:00:00.000000"
    )

    conn = connect_db("DATAWAREHOUSE")

    with conn:
        pass
        # print("currency_df", currency_df)
        # pprint(str(datetime.now()))
        # pprint((dim_date_df["date_id"][0]))
        # create_db_entries_from_df(conn, "dim_currency", dim_currency_df)
        # create_db_entries_from_df(conn, "dim_design", dim_design_df)
        # create_db_entries_from_df(conn, "dim_date", dim_date_df)
