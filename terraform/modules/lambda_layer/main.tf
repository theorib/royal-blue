resource "null_resource" "uv_build" {
  provisioner "local-exec" {
    working_dir = "${path.root}/../"
    command     = "./build_lambda_layer.sh"
  }
  triggers = {
    lock_file_hash = filebase64sha256("${path.root}/../uv.lock")
  }
}

data "archive_file" "layer" {
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.root}/../build/layer"
  output_path      = "${path.root}/../dist/layer.zip"
  depends_on       = [null_resource.uv_build]
}

resource "aws_lambda_layer_version" "layer_version" {
  layer_name          = "royal_blue_layer"
  compatible_runtimes = [var.python_runtime]
  filename            = data.archive_file.layer.output_path
  source_code_hash    = data.archive_file.layer.output_base64sha256
  lifecycle {
    create_before_destroy = true
  }
}
