
resource "aws_s3_bucket" "extract_lambda_layers" {
  bucket_prefix = "lambda-layers-"
}

resource "aws_s3_bucket" "extract_lambda_source_code" {
  bucket_prefix = "lambda-source-code-"
}
