import pandas as pd


def dim_date_dataframe(extracted_dataframes: dict) -> pd.DataFrame:
    date_df = extracted_dataframes.get("date")

    if date_df is None:
        raise ValueError("Error: Missing date table.")

    try:
        dim_date = date_df[[]].drop_duplicates()

        return dim_date

    except Exception as e:
        raise ValueError(f"Error creating dim_date: {e}")
