resource "aws_s3_bucket" "bucket" {
  bucket_prefix = var.s3_bucket_name_prefix
}