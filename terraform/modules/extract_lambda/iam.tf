# ------------------------------
# Lambda IAM Policies for S3 Ingestion Zone Bucket Write
# ------------------------------

# Define
data "aws_iam_policy_document" "ingestion_zone_bucket_put_data_policy_doc" {
  statement {
    effect    = "Allow"
    actions   = ["s3:PutObject"]
    resources = ["${var.ingestion_zone_bucket.arn}/*"]
  }
}

# Create
resource "aws_iam_policy" "ingestion_zone_bucket_put_policy" {
  name_prefix = "s3-policy-${var.lambda_name}-write-"
  policy      = data.aws_iam_policy_document.ingestion_zone_bucket_put_data_policy_doc.json
}

# Attach
resource "aws_iam_role_policy_attachment" "ingestion_zone_bucket_put_policy_attachment" {
  role       = module.extract_lambda.lambda_iam_role.name
  policy_arn = aws_iam_policy.ingestion_zone_bucket_put_policy.arn
}

# ------------------------------
# Lambda IAM Policy for S3 State Bucket Write
# ------------------------------

# Define
data "aws_iam_policy_document" "lambda_state_bucket_put_data_policy_doc" {
  statement {
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:GetObject", "s3:ListBucket", "s3:DeleteObject"]
    resources = ["${var.lambda_state_bucket.arn}/*", "${var.lambda_state_bucket.arn}"]
  }
}

# Create
resource "aws_iam_policy" "lambda_state_bucket_put_policy" {
  name_prefix = "s3-policy-${var.lambda_name}-put-object-"
  policy      = data.aws_iam_policy_document.lambda_state_bucket_put_data_policy_doc.json
}

# Attach
resource "aws_iam_role_policy_attachment" "lambda_state_bucket_put_policy_attachment" {
  role       = module.extract_lambda.lambda_iam_role.name
  policy_arn = aws_iam_policy.lambda_state_bucket_put_policy.arn
}

# # ------------------------------
# # Lambda IAM Policy for S3 State Bucket Read
# # ------------------------------

# # Define
# data "aws_iam_policy_document" "lambda_state_bucket_get_data_policy_doc" {
#   statement {
#     effect    = "Allow"
#     actions   = ["s3:GetObject"]
#     resources = ["${var.lambda_state_bucket.arn}/*"]
#   }
# }

# # Create
# resource "aws_iam_policy" "lambda_state_bucket_get_policy" {
#   name_prefix = "s3-policy-${var.lambda_name}-get-object-"
#   policy      = data.aws_iam_policy_document.lambda_state_bucket_get_data_policy_doc.json
# }

# # Attach
# resource "aws_iam_role_policy_attachment" "lambda_state_bucket_get_policy_attachment" {
#   role       = module.extract_lambda.lambda_iam_role.name
#   policy_arn = aws_iam_policy.lambda_state_bucket_get_policy.arn
# }
