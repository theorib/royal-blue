import pandas as pd


def fact_sales_order_dataframe(extracted_dataframes: dict) -> pd.DataFrame:

    sales_order_df = extracted_dataframes.get("sales_order")

    if sales_order_df is None:
        raise ValueError("Error: Missing sales_order table.")

    try:
        fact_sales_order = sales_order_df[[
            "design_id", 
            "design_name", 
            "file_location", 
            "file_name"]
        ].drop_duplicates()

        return fact_sales_order

    except Exception as e:
        raise ValueError(f"Error creating fact_sales_order: {e}")

# merge(): Combine two Series or DataFrame objects with SQL-style joining
# mg = pd.merge(df1, df2, on="col3", how="left")