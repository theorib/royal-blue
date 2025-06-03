import copy

from state.get_current_state import get_current_state
from state.set_current_state import set_current_state


def update_state(parquet_files, s3_client, bucket_name):
    """
    Args:
        parquet_files (list): List of file metadata dictionaries. Each dict should have:
            - 'table_name': str
            - 'timestamp': str
            - 'file_name': str
            - optionally 'key': str
        s3_client: Boto3 S3 client for accessing S3.
        bucket_name (str): Name of the S3 bucket.

    Returns:
        dict: The updated state.
    """

    current_state = get_current_state(s3_client, bucket_name) or {"ingest_state": []}

    new_state = copy.deepcopy(current_state)
    table_name_record = {
        entry["table_name"]: entry for entry in new_state.get("ingest_state", [])
    }

    for file_entry in parquet_files:
        table_name = file_entry["table_name"]
        existing_entry = table_name_record.get(table_name)
        updated_entry = update_log_entry(existing_entry, file_entry)
        table_name_record[table_name] = updated_entry

    new_state["ingest_state"] = list(table_name_record.values())
    set_current_state(new_state, bucket_name, s3_client)

    return new_state


def build_log_entry(file_entry):
    """
    Constructs a minimal log entry dictionary from a single parquet file entry.

    Args:
        file_entry (dict): Dictionary containing metadata about a single parquet file.
            Expected keys: 'file_name', 'timestamp', optionally 'key'.

    Returns:
        dict: A dictionary representing a single log entry for the ingest log.
    """

    entry = {"file_name": file_entry["file_name"], "timestamp": file_entry["timestamp"]}

    if "key" in file_entry:
        entry["key"] = file_entry["key"]
    return entry


def update_log_entry(existing_entry, file_entry):
    """
    Returns a new updated ingest log entry for a given table without mutating the input.

    Args:
        existing_entry (dict or None): The current ingest state for a table, or None if it doesn't exist.
        file_entry (dict): Metadata for a single parquet file.
            Expected keys: 'table_name', 'timestamp', 'file_name', optionally 'key'.

    Returns:
        dict: A new state entry for the table.
    """
    table_name = file_entry["table_name"]
    log_entry = build_log_entry(file_entry)
    timestamp = file_entry["timestamp"]

    if existing_entry:
        updated_log = existing_entry["ingest_log"] + [log_entry]
        return {
            "table_name": table_name,
            "last_updated": timestamp,
            "ingest_log": updated_log,
        }
    else:
        return {
            "table_name": table_name,
            "last_updated": timestamp,
            "ingest_log": [log_entry],
        }
