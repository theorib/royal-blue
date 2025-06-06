import pandas as pd

from src.lambdas.transform_lambda.dimensions.helpers import (
    dim_staff_dataframe,
)


def transform_dataframes(extracted_dataframes):
    dimension_columns = {
        "staff": dim_staff_dataframe(extracted_dataframes),
        "counterparty": [],
        "location": [
            "location_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        ],
        "design": ["design_id", "design_name", "file_location", "file_name"],
        "currency": ["currency_id", "currency_code", "currency_name"],
    }

    # dimension_dataframes = {}
    # print(f"extracted_dataframes: {extracted_dataframes}")
    # for table_name, columns in dimension_columns.items():
    #     if table_name in extracted_dataframes:
    #         dim_df = extracted_dataframes[table_name][columns].drop_duplicates()
    #         dim_table_name = f"dim_{table_name}" if table_name != "address" else "dim_location"
    #         dimension_dataframes[dim_table_name] = dim_df

    # return dimension_dataframes
