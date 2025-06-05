resource "aws_lambda_function" "generated_lambda" {
  s3_bucket = var.s3_bucket
  s3_key    = var.s3_key
  # filename         = var.filename
  function_name    = var.function_name
  handler          = var.handler
  runtime          = var.runtime
  source_code_hash = var.source_code_hash
  layers           = var.layers
  role             = aws_iam_role.iam_for_lambda.arn
  timeout          = 200
  environment {
    variables = var.environment_variables
  }
}
