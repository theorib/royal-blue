import pandas as pd


def create_fact_sales_order_from_df(
    data_frame: pd.DataFrame,
) -> pd.DataFrame:
    fact_sales_order_df = data_frame.copy()
    # Create columns
    # ! When there is an increal update down the line, what id are we going to use????
    # ! what was the last index used???
    # fact_sales_order_df["sales_record_id"] = range(1, len(fact_sales_order_df) + 1)

    fact_sales_order_df["created_date"] = pd.to_datetime(
        fact_sales_order_df["created_at"]
    ).dt.date
    fact_sales_order_df["last_updated_date"] = pd.to_datetime(
        fact_sales_order_df["last_updated"]
    ).dt.date
    fact_sales_order_df["agreed_payment_date"] = pd.to_datetime(
        fact_sales_order_df["agreed_payment_date"]
    ).dt.date
    fact_sales_order_df["agreed_delivery_date"] = pd.to_datetime(
        fact_sales_order_df["agreed_delivery_date"]
    ).dt.date

    fact_sales_order_df["created_time"] = pd.to_datetime(
        fact_sales_order_df["created_at"]
    ).dt.time
    fact_sales_order_df["last_updated_time"] = pd.to_datetime(
        fact_sales_order_df["last_updated"]
    ).dt.time

    fact_sales_order_df.drop(["created_at", "last_updated"], axis=1, inplace=True)
    fact_sales_order_df.rename(columns={"staff_id": "sales_staff_id"}, inplace=True)

    return fact_sales_order_df
