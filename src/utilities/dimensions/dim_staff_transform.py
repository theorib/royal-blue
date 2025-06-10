import pandas as pd


def dim_staff_dataframe(**dataframes):
    """
    Constructs the staff dimension table by merging staff data with department names.

    This function expects DataFrames for 'staff' and 'department' only.
    It merges staff with their corresponding departments and selects key fields
    for OLAP-friendly dimension usage.

    Parameters:
    -----------
    **dataframes : dict
        A dictionary of named DataFrames, expected to contain:
        - 'staff': DataFrame with at least 'staff_id', 'first_name', 'last_name', 'department_id', 'email_address'
        - 'department': DataFrame with 'department_id', 'department_name' and 'location'

    Returns:
    --------
    pd.DataFrame
        A dimension-style DataFrame containing:
        - 'staff_id'
        - 'first_name'
        - 'last_name'
        - 'department_name'
        - 'email_address'

    Raises:
    -------
    ValueError
        If any of the required dataframes are missing from input.
    Exception
        For any unexpected error encountered during merging or transformation.
    """
    required_keys = ["staff", "department"]

    for key in required_keys:
        if key not in dataframes:
            raise ValueError(f"Error: Missing required dataframe '{key}'.")

    staff_df = dataframes.get("staff")
    departments_df = dataframes.get("department")

    try:
        staff_df = staff_df.merge(
            departments_df[["department_id", "department_name", "location"]],
            how="left",
            on="department_id",
        )

        dim_staff = staff_df[
            [
                "staff_id",
                "first_name",
                "last_name",
                "department_name",
                "location",
                "email_address",
            ]
        ].drop_duplicates()

        return dim_staff

    except KeyError as e:
        raise KeyError(f"Error creating dim_staff: {e}")
    except Exception as e:
        raise Exception(f"Error creating dim_staff: {e}")