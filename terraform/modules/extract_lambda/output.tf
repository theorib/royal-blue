output "lambda" {
  value = aws_lambda_function.lambda
}

output "lambda_function_arn" {
  description = "Expose ARN from nested create_lambda module"
  value       = aws_lambda_function.lambda.arn
}
