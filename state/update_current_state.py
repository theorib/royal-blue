import copy

from get_current_state import get_current_state
from helpers.log_entry import update_log_entry
from set_current_state import set_current_state


def update_state(parquet_files, s3_client, bucket_name):
    """
    Updates the existing ingest log for a table or creates a new one if it doesn't exist.

    Args:
        table_map (dict): A mapping of table names to their corresponding state entries.
        file_entry (dict): A dictionary containing metadata for a single parquet file.
            Expected keys: 'table_name', 'timestamp', 'file_name', optionally 'key'.

    Modifies:
        table_map (dict): Updates the relevant table entry in-place by appending to its ingest log
                          and updating the last_updated timestamp.
    """

    state_response = get_current_state(s3_client, bucket_name)

    if "error" in state_response:
        current_state = {"ingest_state": []}
    else:
        current_state = state_response["success"]["data"]

    new_state = copy.deepcopy(current_state)
    table_map = {entry["table_name"]: entry for entry in new_state.get("ingest_state", [])}

    for file_entry in parquet_files:
        update_log_entry(table_map, file_entry)

    new_state["ingest_state"] = list(table_map.values())
    set_current_state(new_state, bucket_name, s3_client)

    return {
        "success": {
            "message": "State updated successfully.",
            "data": new_state
        }
    }

