from moto import mock_aws
import pytest
import boto3


@pytest.fixture(scope="class")
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name="eu-west-2")
        yield client
