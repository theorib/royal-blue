import json

import boto3
import pytest
from moto import mock_aws

from state.update_current_state import update_state


@pytest.fixture
def s3_fixture():
    with mock_aws():
        s3 = boto3.client("s3", region_name="eu-west-2")
        bucket = "test-bucket"
        s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield s3, bucket


@pytest.fixture
def parquet_files():
    return [
        {
            "table_name": "test_table",
            "file_name": "test_table-2025-05-27_12-00-00_000000",
            "key": "2025/05/27/test_table-2025-05-27_12-00-00_000000",
            "timestamp": "2025-05-27 12:00:00.000000",
        },
        {
            "table_name": "test_table",
            "file_name": "test_table-2025-05-27_12-05-00_000000",
            "timestamp": "2025-05-27 12:05:00.000000",
        },
    ]


def test_update_current_state_creates_new_state_file(s3_fixture, parquet_files):
    s3, bucket = s3_fixture

    response = update_state(parquet_files, s3, bucket)

    assert "ingest_state" in response
    assert len(response["ingest_state"]) == 1

    entry = response["ingest_state"][0]
    assert entry["table_name"] == "test_table"
    assert entry["last_updated"] == "2025-05-27 12:05:00.000000"
    assert len(entry["ingest_log"]) == 2


def test_update_current_state_updates_existing_state(s3_fixture, parquet_files):
    s3, bucket = s3_fixture

    existing_state = {
        "ingest_state": [
            {
                "table_name": "test_table",
                "last_updated": "2025-05-26 10:00:00.000000",
                "ingest_log": [],
            }
        ]
    }

    s3.put_object(
        Bucket=bucket, Key="lambda_state.json", Body=json.dumps(existing_state)
    )

    response = update_state(parquet_files, s3, bucket)

    assert len(response["ingest_state"]) == 1

    entry = response["ingest_state"][0]
    assert entry["last_updated"] == "2025-05-27 12:05:00.000000"
    assert len(entry["ingest_log"]) == 2
