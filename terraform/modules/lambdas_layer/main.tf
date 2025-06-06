resource "null_resource" "layer_build" {
  provisioner "local-exec" {
    working_dir = "${path.root}/../"
    command     = var.build_script_file_path
  }
  triggers = {
    lock_file_hash = filebase64sha256("${path.root}/../uv.lock")
  }
}

data "archive_file" "layer" {
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.root}/../build/layer"
  output_path      = "${path.root}/../dist/${var.layer_zip_filename}"
  depends_on       = [null_resource.layer_build]
}

resource "aws_s3_object" "lambda_layer_object" {
  bucket = var.lambda_layers_bucket.id
  key    = var.layer_zip_filename
  source = data.archive_file.layer.output_path
  etag   = data.archive_file.layer.output_sha256
}

resource "aws_lambda_layer_version" "lambda_layer" {
  layer_name          = var.layer_name
  s3_bucket           = var.lambda_layers_bucket.id
  s3_key              = aws_s3_object.lambda_layer_object.key
  compatible_runtimes = [var.python_runtime]
  source_code_hash    = data.archive_file.layer.output_base64sha256
  lifecycle {
    create_before_destroy = true
  }
}
