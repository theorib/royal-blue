from src.utils import add_to_s3_bucket
from unittest.mock import Mock
from moto import mock_aws
from botocore.exceptions import ClientError
import pytest


@mock_aws
class TestS3AddFunctionality:
    def test_add_to_s3_success(self, s3_client):
        bucket = "test-bucket"
        key = "test_obj.txt"
        body = '{"test": "royal blue s3 bucket"}'

        s3_client.create_bucket(
            Bucket="test-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        response = add_to_s3_bucket(
            client=s3_client, bucket_name=bucket, key=key, body=body
        )

        assert response["Success"]["Message"] == f"File uploaded to s3://{bucket}/{key}"
        assert response["Success"]["Data"] == {"Bucket": bucket, "Key": key}

        s3_obj = s3_client.get_object(Bucket=bucket, Key=key)
        assert s3_obj["Body"].read().decode("utf-8") == body

    @pytest.mark.parametrize(
        "error_code, message",
        [
            ("NoSuchBucket", "The specified bucket does not exist."),
            ("AccessDenied", "Access denied when writing to the S3 bucket."),
            ("InvalidBucketName", "The specified bucket is not valid."),
            ("BucketAlreadyExists", "The bucket name is already in use."),
            ("BucketAlreadyOwnedByYou", "You already own this bucket."),
        ],
    )
    def test_add_to_s3_error_responses(self, s3_client, error_code, message):
        bucket = "non-existant-bucket"
        key = "test_obj"
        body = '{"test": "royal blue s3 bucket failure"}'

        mock_client = Mock()
        mock_client.put_object.side_effect = ClientError(
            error_response={"Error": {"Message": message, "Code": error_code}},
            operation_name="PutObject",
        )

        response = add_to_s3_bucket(
            client=mock_client, bucket_name=bucket, key=key, body=body
        )

        assert "Error" in response
        assert message in response["Error"]["Message"]
