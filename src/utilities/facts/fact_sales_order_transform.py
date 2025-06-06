import pandas as pd


def fact_sales_order_dataframe(extracted_dataframes: dict) -> pd.DataFrame:
    """
    Transforms the extracted 'sales_order' data into a fact_sales_order DataFrame.

    Parameters
    ----------
    extracted_dataframes : dict
        A dictionary of extracted raw tables keyed by table name.

    Returns
    -------
    pd.DataFrame
        The transformed fact_sales_order DataFrame with selected fields.

    Raises
    ------
    ValueError
        If 'sales_order' is missing or required columns are not present.
    """
    sales_df = extracted_dataframes.get("sales_order")

    if sales_df is None:
        raise ValueError("Missing 'sales_order' table from extracted data.")

    required_columns = [
        "sales_order_id",
        "created_at",
        "last_updated",
        "design_id",
        "staff_id",
        "counterparty_id",
        "units_sold",
        "unit_price",
        "currency_id",
        "agreed_delivery_date",
    ]

    if not set(required_columns).issubset(sales_df.columns):
        missing = set(required_columns) - set(sales_df.columns)
        raise ValueError(f"Missing columns in 'sales_order': {missing}")

    fact_sales = sales_df[required_columns].copy()
    return fact_sales
