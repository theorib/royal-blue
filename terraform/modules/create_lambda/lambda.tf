resource "aws_lambda_function" "generated_lambda" {
  filename         = var.filename
  function_name    = var.function_name
  handler          = var.handler
  runtime          = var.runtime
  source_code_hash = var.source_code_hash
  layers           = var.layers
  role             = aws_iam_role.iam_for_lambda.arn

  environment {
    variables = var.environment_variables
  }
}
