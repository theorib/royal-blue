# ------------------------------
# Lambda IAM Policy for S3 Write
# ------------------------------

# Define
data "aws_iam_policy_document" "s3_data_policy_doc" {
  statement {
    effect    = "Allow"
    actions   = ["s3:PutObject"]
    resources = ["${module.ingestion_zone_bucket.s3_bucket.arn}/*"]
  }
}

# Create
resource "aws_iam_policy" "s3_write_policy" {
  name_prefix = "s3-policy-${var.lambda_name}-write-"
  policy      = data.aws_iam_policy_document.s3_data_policy_doc.json
}

# Attach
resource "aws_iam_role_policy_attachment" "lambda_s3_write_policy_attachment" {
  role       = module.extract_lambda.lambda_iam_role.name
  policy_arn = aws_iam_policy.s3_write_policy.arn
}
