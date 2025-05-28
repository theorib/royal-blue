from src.utils import get_file_from_s3_bucket, add_to_s3_bucket
from unittest.mock import Mock
from moto import mock_aws
from botocore.exceptions import ClientError
import pytest

@mock_aws
class TestS3GetFunctionality:
    def test_get_file_from_s3_success(self, s3_client):
        bucket = "test-bucket"
        key = "test_obj.txt"
        body = '{"test": "royal blue s3 bucket"}'

        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        add_to_s3_bucket(
            client=s3_client, bucket_name=bucket, key=key, body=body
        )

        s3_obj = get_file_from_s3_bucket(s3_client, bucket_name=bucket, key=key)
        assert 'File retrieved from s3' in s3_obj["Success"]['Message'] 
        assert len(s3_obj['Success']['Data']) == len(body)

    @pytest.mark.parametrize(
        "error_code, message",
        [
            ("NoSuchBucket", "The specified bucket does not exist."),
            ("NoSuchKey", "The specified key does not exist."),
            ("InvalidBucketName", "The S3 bucket name provided is invalid."),
            ("AccessDenied", "Access denied when reading from the S3 bucket."),
            ("InternalError", "An internal AWS error occurred. Try again."),
            ("SlowDown", "Too many requests sent to S3. Try again later."),
        ],
    )
    def test_add_to_s3_error_responses(self, s3_client, error_code, message):
        bucket = "non-existant-bucket"
        key = "test_obj"

        mock_client = Mock()
        mock_client.get_object.side_effect = ClientError(
            error_response={"Error": {"Message": message, "Code": error_code}},
            operation_name="GetObject",
        )

        response = get_file_from_s3_bucket(
            client=mock_client, bucket_name=bucket, key=key
        )

        assert "Error" in response
        assert message in response["Error"]["Message"]
        assert error_code in response['Error']['Message']


        