output "lambda" {
  value = aws_lambda_function.generated_lambda
}

output "lambda_iam_role" {
  value = aws_iam_role.iam_for_lambda
}
