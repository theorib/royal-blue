output "lambda" {
  value       = aws_lambda_function.generated_lambda
  description = "Terraform aws_lambda_function instance"
}

output "lambda_iam_role" {
  value       = aws_iam_role.iam_for_lambda
  description = "Terraform aws_iam_role attached to the lambda"
}
