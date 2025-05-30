output "extract_lambda" {
  value = module.extract_lambda

}
output "ingest_zone_bucket" {
  value = module.ingestion_zone_bucket
}

output "archive_file" {
  value = data.archive_file.extract_lambda_zip
}

output "lambda_function_arn" {
  description = "Expose ARN from nested create_lambda module"
  value       = module.extract_lambda.lambda_function_arn
}