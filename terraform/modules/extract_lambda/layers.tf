# variable "extract_layer_zip_filename" {
#   default = "layer.zip"
# }

# resource "null_resource" "extract_lambda_layer_build" {
#   provisioner "local-exec" {
#     working_dir = "${path.root}/../"
#     command     = "./build_lambda_layer.sh"
#   }
#   triggers = {
#     lock_file_hash = filebase64sha256("${path.root}/../uv.lock")
#   }
# }

# data "archive_file" "extract_layer" {
#   type             = "zip"
#   output_file_mode = "0666"
#   source_dir       = "${path.root}/../build/layer"
#   output_path      = "${path.root}/../dist/${var.extract_layer_zip_filename}"
#   depends_on       = [null_resource.extract_lambda_layer_build]
# }

# resource "aws_s3_object" "lambda_layer_object" {
#   # for_each = { for lib in var.libraries : lib.name => lib }

#   bucket = var.lambda_layers_bucket.id
#   key    = var.extract_layer_zip_filename
#   source = data.archive_file.extract_layer.output_path
#   etag   = data.archive_file.extract_layer.output_sha256
# }

# resource "aws_lambda_layer_version" "extract_lambda" {
#   layer_name          = "royal_blue_layer"
#   s3_bucket           = var.lambda_layers_bucket.id
#   s3_key              = aws_s3_object.lambda_layer_object.key
#   compatible_runtimes = [var.python_runtime]
#   # filename            = data.archive_file.extract_layer.output_path
#   source_code_hash = data.archive_file.extract_layer.output_base64sha256
#   lifecycle {
#     create_before_destroy = true
#   }
# }
# # data "archive_file" "extract_lambda_zip" {
# #   type             = "zip"
# #   output_file_mode = "0666"
# #   source_dir       = "${path.root}/../build/extract_lambda/"
# #   # source_file      = "${path.root}/../src/lambdas/extract_lambda/extract_lambda.py"
# #   output_path = "${path.root}/../dist/extract_lambda.zip"

# # }
