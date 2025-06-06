resource "aws_s3_bucket" "ingest_zone" {
  bucket_prefix = "ingestion-zone-"
  force_destroy = true
}

resource "aws_s3_bucket" "processed_zone" {
  bucket_prefix = "processed-zone-"
  force_destroy = true
}

resource "aws_s3_bucket" "lambda_state" {
  bucket_prefix = "lambda-state-"
  force_destroy = true
}


resource "aws_s3_bucket" "lambda_source_code" {
  bucket_prefix = "lambda-source-code-"
  force_destroy = true
}


resource "aws_s3_bucket" "extract_lambda_layers" {
  bucket_prefix = "lambda-layers-"
  force_destroy = true
}
