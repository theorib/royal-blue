output "extract_lambda" {
  value = aws_lambda_function.extract_lambda
}

output "lambda_function_arn" {
  description = "Expose ARN from nested create_lambda module"
  value       = aws_lambda_function.extract_lambda.arn
}
