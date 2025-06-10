import pandas as pd


def dim_staff_dataframe(**dataframes):
    """
    Constructs the staff dimension table by integrating staff, department, address,
    and purchase order data into a clean OLAP-ready format.

    This function expects dataframes for 'staff', 'department', 'address', and 'purchase_order',
    and selects then returns relevant OLAP fields for the staff dimension.

    Parameters:
    -----------
    **dataframes : dict
        A dictionary of named DataFrames, expected to contain:
        - 'staff': DataFrame with at least 'staff_id', 'first_name', 'last_name', 'department_id', 'email_address'
        - 'department': DataFrame with 'department_id' and 'department_name'
        - 'address': DataFrame with 'address_id', 'city', and 'country'
        - 'purchase_order': DataFrame with 'staff_id' and 'agreed_delivery_location_id'

    Returns:
    --------
    pd.DataFrame
        A dimension-style DataFrame containing:
        - 'staff_id'
        - 'first_name'
        - 'last_name'
        - 'department_name'
        - 'location' (formatted as "City, Country")
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
    address_df = dataframes.get("address")
    purchase_order_df = dataframes.get("purchase_order")

    try:
        staff_df = staff_df.merge(
            departments_df[["department_id", "department_name"]],
            how="left",
            on="department_id",
        )

        staff_orders = purchase_order_df[
            ["staff_id", "agreed_delivery_location_id"]
        ].drop_duplicates()
        staff_df = staff_df.merge(
            staff_orders, how="left", left_on="staff_id", right_on="staff_id"
        )

        staff_df = staff_df.merge(
            address_df[["address_id", "city", "country"]],
            how="left",
            left_on="agreed_delivery_location_id",
            right_on="address_id",
        )

        staff_df["location"] = (
            staff_df["city"].fillna("Unknown")
            + ", "
            + staff_df["country"].fillna("Unknown")
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
        ]

        return dim_staff
    except Exception as e:
        raise e
