from typing import TypedDict


class EmptyDict(TypedDict):
    pass


# name of bucket will come from an environment variable
# LAMBDA_STATE_BUCKET_NAME
# file_name = lambda_state.json

# get_current_state()
# connect to an s3bucket and pull a specific json file
# convert from json to python dictionary
# return that dictionary

# set_current_state(current_state:dict)
# connect to s3
# convert current_state to json
# create a json file in s3 with specific name
