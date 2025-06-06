# ------------------------------
# Lambda IAM Role Creation
# ------------------------------
data "aws_iam_policy_document" "assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "iam_for_lambda" {
  name_prefix        = "iam_for_${var.function_name}_"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}


# ------------------------------
# Lambda IAM Policies for Lambda Logging on CloudWatch
# ------------------------------

# Define
data "aws_iam_policy_document" "lambda_logging" {
  statement {
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup"]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"]
  }
  statement {
    effect = "Allow"

    actions = [

      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]

    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.function_name}:*"]
  }
}

# Create
resource "aws_iam_policy" "lambda_logging" {
  name_prefix = "lambda-${var.function_name}-logging-"
  policy      = data.aws_iam_policy_document.lambda_logging.json

}
# Attach
resource "aws_iam_role_policy_attachment" "lambda_logging" {
  role       = aws_iam_role.iam_for_lambda.name
  policy_arn = aws_iam_policy.lambda_logging.arn
}


# ------------------------------
# Lambda IAM Policies for S3 Ingestion Zone Bucket Write
# ------------------------------

# Define
data "aws_iam_policy_document" "ingest_zone_bucket_policy_doc" {
  statement {
    effect    = "Allow"
    actions   = ["s3:PutObject"]
    resources = ["${var.ingest_zone_bucket.arn}/*"]
  }
}

# Create
resource "aws_iam_policy" "ingest_zone_bucket_policy" {
  name_prefix = "s3-policy-${var.function_name}-write-"
  policy      = data.aws_iam_policy_document.ingest_zone_bucket_policy_doc.json
}

# Attach
resource "aws_iam_role_policy_attachment" "ingest_zone_bucket_policy_attachment" {
  role       = aws_iam_role.iam_for_lambda.name
  policy_arn = aws_iam_policy.ingest_zone_bucket_policy.arn
}

# ------------------------------
# Lambda IAM Policy for S3 State Bucket Write
# ------------------------------

# Define
data "aws_iam_policy_document" "lambda_state_bucket_policy_doc" {
  statement {
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:GetObject", "s3:ListBucket", "s3:DeleteObject"]
    resources = ["${var.lambda_state_bucket.arn}/*", "${var.lambda_state_bucket.arn}"]
  }
}

# Create
resource "aws_iam_policy" "lambda_state_bucket_policy" {
  name_prefix = "s3-policy-${var.function_name}-put-object-"
  policy      = data.aws_iam_policy_document.lambda_state_bucket_policy_doc.json
}

# Attach
resource "aws_iam_role_policy_attachment" "lambda_state_bucket_policy_attachment" {
  role       = aws_iam_role.iam_for_lambda.name
  policy_arn = aws_iam_policy.lambda_state_bucket_policy.arn
}
