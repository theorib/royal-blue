import boto3
import pytest
from moto import mock_aws


@pytest.fixture(scope="function")
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name="eu-west-2")
        yield client
