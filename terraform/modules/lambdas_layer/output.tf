output "aws_lambda_layer_version" {
  value = aws_lambda_layer_version.lambda_layer
}

output "lambda_layer_object" {
  value = aws_s3_object.lambda_layer_object
}

output "layer_archive_file" {
  value = data.archive_file.layer
}

output "layer_zip_filename" {
  value = var.layer_zip_filename
}

output "layer_build_resource" {
  value = null_resource.layer_build
}
