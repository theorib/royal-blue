import pandas as pd


def dim_location_dataframe(dataframes: dict) -> pd.DataFrame:
    """
    Create the location dimension DataFrame from the extracted raw address data.

    Parameters:
    -----------
    dataframes : dict
        Dictionary containing raw DataFrames extracted from source tables.
        Must contain an 'address' DataFrame.

    Returns:
    --------
    pd.DataFrame
        A DataFrame representing the location dimension with columns:
        'address_id', 'address_line_1', 'address_line_2', 'district', 'city',
        'postal_code', 'country', and 'phone'.

    Raises:
    -------
    ValueError
        If the 'address' table is missing or any error occurs during transformation.
    """

    address_df = dataframes.get("address")

    if address_df is None:
        raise ValueError("Error: Missing address table.")

    try:
        dim_location = address_df[
            [
                "address_id",
                "address_line_1",
                "address_line_2",
                "district",
                "city",
                "postal_code",
                "country",
                "phone",
            ]
        ].drop_duplicates()

        return dim_location

    except Exception as e:
        raise ValueError(f"Error creating dim_location: {e}")
