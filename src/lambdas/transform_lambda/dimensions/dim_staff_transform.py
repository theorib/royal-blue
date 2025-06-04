import pandas as pd


def dim_staff_dataframe(extracted_dataframes: dict):
    staff_df = extracted_dataframes.get('staff')
    departments_df = extracted_dataframes['department']
    locations_df = extracted_dataframes['address']
    purchase_order_df = extracted_dataframes['purchase_order']

    if staff_df is None:
        raise ValueError('Error:')
    if departments_df is None:
        raise ValueError('Error:')
    
    
    try:
        staff_df = staff_df.merge(
            departments_df[['department_id', 'department_name']],
            how="left",
            on="department_id"
        )
        
         # Merge staff ←→ purchase_order to get delivery location ID
        staff_orders = purchase_order_df[['staff_id', 'agreed_delivery_location_id']].drop_duplicates()
        staff_df = staff_df.merge(
            staff_orders,
            how='left',
            left_on='staff_id',
            right_on='staff_id'
        )

        # Merge location ID ←→ address table
        staff_df = staff_df.merge(
            locations_df[['address_id', 'city', 'country']],
            how='left',
            left_on='agreed_delivery_location_id',
            right_on='address_id'
        )

        # Create a readable location string
        staff_df['location'] = staff_df['city'].fillna('Unknown') + ', ' + staff_df['country'].fillna('Unknown')


        dim_staff = staff_df[[
            'staff_id',
            'first_name',
            'last_name',
            'department_name',
            'location',
            'email_address'
        ]]
        print(dim_staff)
        return dim_staff
    except Exception as e:
        raise e
    