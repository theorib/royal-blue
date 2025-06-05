import pandas as pd


def dim_counterparty_dataframe(extracted_dataframes: dict):
    """
    Transforms and returns a dimension dataframe for counterparties by merging with address data.

    This function takes a dictionary of extracted dataframes, expects 'counterparty' and 'address' keys,
    merges the counterparty data with the corresponding legal address details, renames the address fields
    to OLAP-compliant snake_case names, and selects a subset of relevant columns.

    Parameters:
        extracted_dataframes (dict): A dictionary containing:
            - 'counterparty': pd.DataFrame with at least 'counterparty_id', 'counterparty_legal_name', and 'legal_address_id'.
            - 'address': pd.DataFrame with address information including 'legal_address_id' and address fields.

    Returns:
        pd.DataFrame: A transformed dimension dataframe containing counterparty and legal address fields.

    Raises:
        ValueError: If either the 'counterparty' or 'address' dataframe is missing.
        KeyError: If required columns are missing from the 'address' dataframe.
        Exception: Propagates any other unexpected exception that occurs during processing.
    """
    counterparty_df = extracted_dataframes.get("counterparty")
    address_df = extracted_dataframes.get("address")

    if counterparty_df is None:
        raise ValueError("Error: Missing counterparty table.")
    if address_df is None:
        raise ValueError("Error: Missing address table.")

    try:
        counterparty_df = counterparty_df.merge(
            address_df[
                [
                    "legal_address_id",
                    "address_line_1",
                    "address_line_2",
                    "district",
                    "city",
                    "postal_code",
                    "country",
                    "phone",
                ]
            ],
            on="legal_address_id",
        )

        counterparty_df = counterparty_df.rename(
            columns={
                "address_line_1": "counterparty_legal_address_line_1",
                "address_line_2": "counterparty_legal_address_line_2",
                "district": "counterparty_legal_district",
                "city": "counterparty_legal_city",
                "postal_code": "counterparty_legal_postal_code",
                "country": "counterparty_legal_country",
                "phone": "counterparty_legal_phone_number",
            }
        )

        dim_counterparty = counterparty_df[
            [
                "counterparty_id",
                "counterparty_legal_name",
                "counterparty_legal_address_line_1",
                "counterparty_legal_address_line_2",
                "counterparty_legal_district",
                "counterparty_legal_city",
                "counterparty_legal_postal_code",
                "counterparty_legal_country",
                "counterparty_legal_phone_number",
            ]
        ]

        return dim_counterparty
    except Exception as e:
        raise e
