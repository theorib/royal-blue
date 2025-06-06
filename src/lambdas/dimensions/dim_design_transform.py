import pandas as pd


def dim_design_dataframe(dataframes: dict) -> pd.DataFrame:
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

    design_df = dataframes.get("design")

    if design_df is None:
        raise ValueError("Error: Missing design table.")

    try:
        dim_design = design_df[
            ["design_id", "design_name", "file_location", "file_name"]
        ].drop_duplicates()

        return dim_design

    except Exception as e:
        raise ValueError(f"Error creating dim_design: {e}")
