import copy

from src.utilities.state.get_current_state import get_current_state
from src.utilities.state.set_current_state import set_current_state


def update_state(parquet_files, s3_client, bucket_name)-> dict:
    """
    Updates the ingest state in S3 with new parquet file metadata.

    Args:
        parquet_files (list of dict): Each dict must contain:
            - 'table_name' (str)
            - 'timestamp' (str)
            - 'file_name' (str)
            - optionally 'key' (str)
        s3_client: Boto3 S3 client instance.
        bucket_name (str): S3 bucket name.

    Returns:
        dict: The updated ingest state dictionary.
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


def build_log_entry(file_entry) -> dict:
    """
    Builds a log entry dict from a parquet file metadata dict.

    Args:
        file_entry (dict): Metadata for a parquet file, with keys:
            'file_name', 'timestamp', optionally 'key'.

    Returns:
        dict: Minimal log entry with file_name, timestamp, and optional key.
    """

    entry = {"file_name": file_entry["file_name"], "timestamp": file_entry["timestamp"]}

    if "key" in file_entry:
        entry["key"] = file_entry["key"]
    return entry


def update_log_entry(existing_entry, file_entry) -> dict:
    """
    Creates a new ingest log entry for a table, appending new file metadata.

    Args:
        existing_entry (dict or None): Current ingest state entry for a table.
        file_entry (dict): Parquet file metadata including 'table_name', 'timestamp', 'file_name', optionally 'key'.

    Returns:
        dict: Updated ingest state entry with appended log and updated last_updated.
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
