output "extract_lambda" {
  value = module.extract_lambda

}
output "ingest_zone_bucket" {
  value = module.ingest_zone_bucket
}

output "archive_file" {
  value = data.archive_file.extract_lambda_zip
}
