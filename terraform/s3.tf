resource "aws_s3_bucket" "ingestion_zone" {
  bucket_prefix = "ingestion-zone-"
}

resource "aws_s3_bucket" "process_zone" {
  bucket_prefix = "processed-zone-"
}

resource "aws_s3_bucket" "lambda_state" {
  bucket_prefix = "lambda-state-"
}


