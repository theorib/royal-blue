from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from src.utils import add_to_s3_bucket


@pytest.mark.describe("add_to_s3_bucket Utility Function Behaviour")
@mock_aws
class TestS3AddFunctionality:
    @pytest.mark.it(
        "Should check that an object is successfully uploaded to the S3 bucket"
    )
    def test_add_to_s3_200_success(self, s3_client):
        bucket = "test-bucket"
        key = "test_obj.txt"
        body = '{ "test": "royal blue s3 bucket" }'

        s3_client.create_bucket(
            Bucket="test-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        response = add_to_s3_bucket(
            client=s3_client, bucket_name=bucket, key=key, body=body
        )

        assert response["success"]["message"] == f"File uploaded to s3://{bucket}/{key}"
        assert response["success"]["data"] == {"bucket": bucket, "key": key}

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
    @pytest.mark.it(
        "Should check that errors are handled correctly when uploading to an S3 bucket"
    )
    def test_add_to_s3_error_responses(self, error_code, message):
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

        assert "error" in response
        assert message in response["error"]["message"]
