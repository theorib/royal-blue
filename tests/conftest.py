import os

import boto3
import pytest
from moto import mock_aws


@pytest.fixture(scope="module")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"
    
@pytest.fixture(scope="function")
def set_testdb_env(monkeypatch):
    monkeypatch.setenv("DB_USER", "user")
    monkeypatch.setenv("DB_PASSWORD", "pass")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_DATABASE", "testdb")
    monkeypatch.setenv("DB_PORT", "5432")
    
@pytest.fixture(scope="function")
def aws_s3_bucket_client():
    with mock_aws():
        bucket_client = boto3.client("s3")
        yield bucket_client

@pytest.fixture(scope="function")
def aws_bucket_from_client(aws_s3_bucket_client):
    with mock_aws():
        bucket = aws_s3_bucket_client.create_bucket(Bucket="Test-bucket")
        yield bucket