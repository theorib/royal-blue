output "archive_file" {
  value = data.archive_file.layer
}

output "aws_lambda_layer_version" {
  value = aws_lambda_layer_version.layer_version
}
