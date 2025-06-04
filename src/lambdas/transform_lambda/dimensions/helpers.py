
def dim_staff_dataframe(dataframe):
    dim_staff = dataframe.rename(columns={
            "staff_id": "staff_id",
            "first_name": "staff_first_name",
            "last_name": "staff_last_name",
            "department_name": "staff_department_name",
            "location": "staff_location",
            "email_address": "staff_email_address"
        }).drop_duplicates()